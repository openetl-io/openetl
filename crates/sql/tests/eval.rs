//! Plans evaluated on batches, binding again when a schema changes, and the
//! catalog's checks.

use std::collections::HashMap;
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

/// A stream of documents: `doc` a JSON column, `raw` the same text as a
/// plain string, and `field` a key per row.
fn events() -> (Catalog, RecordBatch) {
    let json = HashMap::from([
        ("ARROW:extension:name".to_owned(), "arrow.json".to_owned()),
        ("ARROW:extension:metadata".to_owned(), String::new()),
    ]);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("doc", DataType::Utf8, true).with_metadata(json),
        Field::new("raw", DataType::Utf8, true),
        Field::new("field", DataType::Utf8, true),
    ]));
    let docs = vec![
        Some(
            r#"{"name": "a", "n": 7.0, "status": "active", "deleted": null,
                "user": {"email": "e@x.io", "tags": ["x", "y"]},
                "big": 18446744073709551615, "amount": 12345678901234567.89}"#,
        ),
        Some(r#"{"name": null, "n": "8", "status": "gone", "user": [1, 2]}"#),
        Some(r#"{"name": "c", "n": 9"#),
        None,
    ];
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
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(docs.clone())),
            Arc::new(StringArray::from(docs)),
            Arc::new(StringArray::from(vec![
                Some("name"),
                Some("status"),
                Some("name"),
                None,
            ])),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    (c, batch)
}

fn ints(b: &RecordBatch, column: &str) -> Vec<Option<i64>> {
    let i = b
        .schema()
        .index_of(column)
        .unwrap_or_else(|e| panic!("{e}"));
    b.column(i).as_primitive::<Int64Type>().iter().collect()
}

fn is_json(s: &Schema, column: &str) -> bool {
    s.field_with_name(column).is_ok_and(|f| {
        f.metadata().get("ARROW:extension:name").map(String::as_str) == Some("arrow.json")
    })
}

#[test]
fn json_paths_read_json_columns_and_strings() {
    let (c, batch) = events();
    let s = plan_one(
        "INSERT INTO x SELECT id,
           doc->>'name' AS name,
           doc->'user'->>'email' AS email,
           (doc->'n')::bigint AS n,
           doc->'user' AS user_json,
           doc->'big' AS big,
           doc->'amount' AS amount,
           doc->'deleted' AS deleted,
           json_get_json(doc, 'deleted') AS deleted_json,
           json_get_str(raw, 'user', 'tags', -1) AS last_tag,
           raw #>> ARRAY['user', 'tags', '0'] AS first_tag,
           json_as_text(doc, field) AS by_field,
           doc ? 'deleted' AS has_deleted,
           json_length(raw, 'user') AS user_length
         FROM events",
        &c,
    );
    let out = run(row_wise(&s), &batch);
    assert_eq!(out.schema(), s.schema);
    let some = |v: &[Option<&str>]| -> Vec<Option<String>> {
        v.iter().map(|s| s.map(str::to_owned)).collect()
    };
    // A JSON null is null in ->>'s text; a document that isn't JSON
    // throughout has nothing in it, and a null one neither.
    assert_eq!(strings(&out, "name"), some(&[Some("a"), None, None, None]));
    assert_eq!(
        strings(&out, "email"),
        some(&[Some("e@x.io"), None, None, None])
    );
    assert_eq!(ints(&out, "n"), [Some(7), Some(8), None, None]);
    // An output column holds -> as JSON text, as the document writes it.
    for column in ["user_json", "big", "amount", "deleted", "deleted_json"] {
        assert!(is_json(&s.schema, column), "{column}");
    }
    assert!(!is_json(&s.schema, "name"));
    assert_eq!(
        strings(&out, "user_json"),
        some(&[
            Some(r#"{"email": "e@x.io", "tags": ["x", "y"]}"#),
            Some("[1, 2]"),
            None,
            None
        ])
    );
    assert_eq!(
        strings(&out, "big"),
        some(&[Some("18446744073709551615"), None, None, None])
    );
    assert_eq!(
        strings(&out, "amount"),
        some(&[Some("12345678901234567.89"), None, None, None])
    );
    assert_eq!(strings(&out, "deleted"), some(&[None, None, None, None]));
    assert_eq!(
        strings(&out, "deleted_json"),
        some(&[Some("null"), None, None, None])
    );
    assert_eq!(
        strings(&out, "last_tag"),
        some(&[Some("y"), None, None, None])
    );
    assert_eq!(
        strings(&out, "first_tag"),
        some(&[Some("x"), None, None, None])
    );
    assert_eq!(
        strings(&out, "by_field"),
        some(&[Some("a"), Some("gone"), None, None])
    );
    let has: Vec<Option<bool>> = out
        .column(
            s.schema
                .index_of("has_deleted")
                .unwrap_or_else(|e| panic!("{e}")),
        )
        .as_boolean()
        .iter()
        .collect();
    assert_eq!(has, [Some(true), Some(false), Some(false), Some(false)]);
    assert_eq!(ints(&out, "user_length"), [Some(2), Some(2), None, None]);
}

#[test]
fn json_values_filter_and_pass_through_views() {
    let (c, batch) = events();
    let s = plan_one(
        "INSERT INTO x SELECT id FROM events WHERE doc->'status' = 'active' OR doc ? 'user'",
        &c,
    );
    let out = run(row_wise(&s), &batch);
    assert_eq!(ints(&out, "id"), [Some(1), Some(2)]);
    let s = plan_one(
        "INSERT INTO x SELECT id FROM events WHERE doc->'name' IS NULL",
        &c,
    );
    let out = run(row_wise(&s), &batch);
    assert_eq!(ints(&out, "id"), [Some(2), Some(3), Some(4)]);
    let plan = openetl_sql::plan(
        "CREATE VIEW users AS SELECT id, doc->'user' AS u FROM events;
         INSERT INTO x SELECT id, u->>'email' AS email, json_get_json(u, 'tags', 1) AS tag FROM users",
        &c,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let out = run(row_wise(&plan.statements[0]), &batch);
    assert_eq!(
        strings(&out, "email"),
        [Some("e@x.io".to_owned()), None, None, None]
    );
    assert_eq!(
        strings(&out, "tag"),
        [Some(r#""y""#.to_owned()), None, None, None]
    );
}

#[test]
fn a_wide_stream_plans() {
    let fields: Vec<Field> = (0..2_000)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
        .collect();
    let mut c = Catalog::new();
    c.insert(
        "wide",
        Stream {
            schema: Arc::new(Schema::new(fields)),
            key: Vec::new(),
            changelog: false,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let s = plan_one("INSERT INTO x SELECT * FROM wide", &c);
    assert_eq!(s.schema.fields().len(), 2_000);
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
