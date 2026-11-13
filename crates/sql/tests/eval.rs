//! Plans evaluated on batches, binding again when a schema changes, and the
//! catalog's checks.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use openetl_sql::{Catalog, ErrorKind, Operator, RowWise, Sql, Statement, Step, Stream};

fn customers(schema: Vec<Field>) -> Catalog {
    let mut c = Catalog::new();
    c.insert(
        "customers",
        Stream {
            schema: Arc::new(Schema::new(schema)),
            key: vec!["id".into()],
            changelog: false,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    c
}

fn fields() -> Vec<Field> {
    vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]
}

fn plan_one(sql: &str, catalog: &Catalog) -> Statement {
    let mut plan = openetl_sql::plan(sql, catalog).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(plan.statements.len(), 1);
    plan.statements.remove(0)
}

fn row_wise(s: &Statement) -> &RowWise {
    let Operator::RowWise(op) = &s.operator;
    op
}

/// Runs a statement's steps on a batch, keeping rows that pass each filter.
fn run(op: &RowWise, batch: &RecordBatch) -> RecordBatch {
    let mut current = op.input(batch).unwrap_or_else(|e| panic!("{e}"));
    for step in &op.steps {
        current = match step {
            Step::Filter(p) => {
                let mask = p.eval_predicate(&current).unwrap_or_else(|e| panic!("{e}"));
                arrow_select::filter::filter_record_batch(&current, &mask)
                    .unwrap_or_else(|e| panic!("{e}"))
            }
            Step::Project(p) => p.eval(&current).unwrap_or_else(|e| panic!("{e}")),
            Step::Unnest(_) => panic!("no unnest here"),
        };
    }
    current
}

/// A batch of the stream with its columns in another order, and one the
/// statement doesn't read.
fn batch(ids: &[i64], emails: &[Option<&str>]) -> RecordBatch {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("_openetl_op", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]));
    let n = ids.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["insert"; n])) as ArrayRef,
            Arc::new(StringArray::from(emails.to_vec())),
            Arc::new(StringArray::from(vec![Some("n"); n])),
            Arc::new(Int64Array::from(ids.to_vec())),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

fn strings(b: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let i = b
        .schema()
        .index_of(column)
        .unwrap_or_else(|e| panic!("{e}"));
    b.column(i)
        .as_string::<i32>()
        .iter()
        .map(|s| s.map(str::to_owned))
        .collect()
}

#[test]
fn a_statement_evaluates_by_column_name() {
    let c = customers(fields());
    let s = plan_one(
        "INSERT INTO customers SELECT id, lower(email) AS email, 'eu' AS region FROM customers",
        &c,
    );
    let out = run(row_wise(&s), &batch(&[1, 2], &[Some("A@X.io"), None]));
    assert_eq!(out.schema(), s.schema);
    assert_eq!(strings(&out, "email"), [Some("a@x.io".into()), None]);
    assert_eq!(
        strings(&out, "region"),
        [Some("eu".into()), Some("eu".into())]
    );
}

#[test]
fn a_predicate_that_is_null_is_false() {
    let c = customers(fields());
    let s = plan_one(
        "INSERT INTO x SELECT id FROM customers WHERE email LIKE '%@x.io'",
        &c,
    );
    let op = row_wise(&s);
    let Some(Step::Filter(p)) = op.steps.first() else {
        panic!("{:?}", op.steps)
    };
    let input = op
        .input(&batch(&[1, 2, 3], &[Some("a@x.io"), None, Some("b@y.io")]))
        .unwrap_or_else(|e| panic!("{e}"));
    let mask = p.eval_predicate(&input).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(mask.null_count(), 0);
    assert_eq!(
        mask.iter().flatten().collect::<Vec<_>>(),
        [true, false, false]
    );
    let out = run(
        op,
        &batch(&[1, 2, 3], &[Some("a@x.io"), None, Some("b@y.io")]),
    );
    let ids: Vec<i64> = out
        .column(0)
        .as_primitive::<Int64Type>()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids, [1]);
}

#[test]
fn now_is_when_the_batch_is_evaluated() {
    let c = customers(fields());
    let s = plan_one("INSERT INTO x SELECT id, now() AS seen FROM customers", &c);
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let out = run(row_wise(&s), &batch(&[1, 2], &[None, None]));
    let seen = out.column(1).as_primitive::<TimestampMicrosecondType>();
    let first =
        u128::try_from(seen.iter().flatten().next().unwrap_or_default()).unwrap_or_default();
    assert!(first >= before, "{first} < {before}");
    let Some(Step::Project(p)) = row_wise(&s).steps.last() else {
        panic!("{:?}", row_wise(&s).steps)
    };
    let deterministic: Vec<bool> = p
        .columns
        .iter()
        .map(|c| c.expr.is_deterministic())
        .collect();
    assert_eq!(deterministic, [true, false]);
}

#[test]
fn an_output_column_is_cast_to_a_logical_type() {
    let c = customers(fields());
    let s = plan_one(
        "INSERT INTO x SELECT id, md5(email) AS h FROM customers",
        &c,
    );
    assert_eq!(s.schema.field(1).data_type(), &DataType::Utf8);
    let out = run(row_wise(&s), &batch(&[1], &[Some("a")]));
    assert_eq!(
        strings(&out, "h"),
        [Some("0cc175b9c0f1b6a831c399e269772661".into())]
    );
}

#[test]
fn a_batch_without_a_column_read_or_with_another_type_is_refused() {
    let c = customers(fields());
    let s = plan_one("INSERT INTO x SELECT id, email FROM customers", &c);
    let narrow = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let e = row_wise(&s).input(&narrow).err();
    assert_eq!(e.map(|e| e.kind), Some(ErrorKind::Eval));
}

/// What P3.9 relies on: the SQL parses once and binds again against a
/// changed schema. A new column reaches an output only through `*`, and a
/// widened one retypes what uses it.
#[test]
fn statements_bind_again_when_a_schema_changes() {
    let sql = Sql::parse(
        "INSERT INTO everything SELECT * FROM customers;
         INSERT INTO some SELECT id, email FROM customers;
         INSERT INTO sums SELECT id, abs(score) AS next FROM customers",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let mut v1 = fields();
    v1.push(Field::new("score", DataType::Int32, true));
    let before = sql.plan(&customers(v1)).unwrap_or_else(|e| panic!("{e}"));
    let mut v2 = fields();
    v2.push(Field::new("score", DataType::Int64, true));
    v2.push(Field::new("country", DataType::Utf8, true));
    let after = sql.plan(&customers(v2)).unwrap_or_else(|e| panic!("{e}"));
    let names = |s: &Statement| -> Vec<String> {
        s.schema.fields().iter().map(|f| f.name().clone()).collect()
    };
    assert_eq!(names(&before.statements[0]).len(), 4);
    assert_eq!(names(&after.statements[0]).len(), 5);
    assert_eq!(names(&before.statements[1]), names(&after.statements[1]));
    assert_eq!(
        before.statements[2].schema.field(1).data_type(),
        &DataType::Int32
    );
    assert_eq!(
        after.statements[2].schema.field(1).data_type(),
        &DataType::Int64
    );
}

#[test]
fn json_paths_give_null_for_a_missing_path_a_string_that_isnt_json_and_null() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("doc", DataType::Utf8, true),
    ]));
    let mut c = Catalog::new();
    c.insert(
        "events",
        Stream {
            schema: schema.clone(),
            key: vec!["id".into()],
            changelog: false,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let s = plan_one(
        "INSERT INTO x SELECT id, doc->>'name' AS name, json_get_int(doc, 'n') AS n,
         doc->'user'->>'email' AS email, json_contains(doc, 'n') AS has_n FROM events",
        &c,
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"name": "a", "n": 7, "user": {"email": "e@x.io"}}"#),
                Some(r#"{"other": 1}"#),
                Some("not json"),
                None,
            ])),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let out = run(row_wise(&s), &batch);
    assert_eq!(strings(&out, "name"), [Some("a".into()), None, None, None]);
    assert_eq!(
        strings(&out, "email"),
        [Some("e@x.io".into()), None, None, None]
    );
    let n: Vec<Option<i64>> = out.column(2).as_primitive::<Int64Type>().iter().collect();
    assert_eq!(n, [Some(7), None, None, None]);
    let has_n: Vec<Option<bool>> = out.column(4).as_boolean().iter().collect();
    assert_eq!(has_n, [Some(true), Some(false), Some(false), Some(false)]);
}

#[test]
fn the_catalog_refuses_a_name_twice_and_a_key_it_lacks() {
    let mut c = customers(fields());
    let stream = |key: &str| Stream {
        schema: Arc::new(Schema::new(fields())),
        key: vec![key.into()],
        changelog: false,
    };
    let e = c.insert("customers", stream("id")).err();
    assert_eq!(e.map(|e| e.kind), Some(ErrorKind::Catalog));
    let e = c.insert("other", stream("nope")).err();
    assert_eq!(e.map(|e| e.kind), Some(ErrorKind::Catalog));
    assert!(c.insert("Customers", stream("id")).is_ok());
}

#[test]
fn errors_come_in_the_order_of_their_positions() {
    let c = customers(fields());
    let e = openetl_sql::plan(
        "INSERT INTO a SELECT nope FROM customers;\nINSERT INTO b SELECT id FROM customers ORDER BY id",
        &c,
    )
    .err()
    .unwrap_or_else(|| panic!("planned"));
    let at: Vec<_> = e.iter().map(|e| e.at.map(|p| (p.line, p.column))).collect();
    assert_eq!(at, [Some((1, 22)), Some((2, 49))]);
    assert_eq!(
        e.to_string(),
        "1:22: there's no column nope; there are id, email, name\n\
         2:49: a stream has no order, so ORDER BY is refused"
    );
}
