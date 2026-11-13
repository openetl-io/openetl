//! Plans evaluated on batches, binding again when a schema changes, and the
//! catalog's checks.

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, ListBuilder, StringBuilder};
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
            Step::Unnest(u) => u.eval(&current).unwrap_or_else(|e| panic!("{e}")).batch,
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

fn events() -> Catalog {
    let list = |t| DataType::List(Arc::new(Field::new_list_field(t, true)));
    let mut c = Catalog::new();
    c.insert(
        "events",
        Stream {
            schema: Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Int64, true),
                Field::new("tags", list(DataType::Utf8), true),
                Field::new("scores", list(DataType::Int64), true),
            ])),
            key: Vec::new(),
            changelog: false,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    c
}

/// Rows of user_id, tags and scores; None is a null list.
type Event<'a> = (i64, Option<&'a [&'a str]>, Option<&'a [i64]>);

fn event_batch(rows: &[Event]) -> RecordBatch {
    let mut tags = ListBuilder::new(StringBuilder::new());
    let mut scores = ListBuilder::new(Int64Builder::new());
    for (_, t, s) in rows {
        match t {
            Some(t) => {
                for v in *t {
                    tags.values().append_value(v);
                }
                tags.append(true);
            }
            None => tags.append_null(),
        }
        match s {
            Some(s) => {
                for v in *s {
                    scores.values().append_value(*v);
                }
                scores.append(true);
            }
            None => scores.append_null(),
        }
    }
    let ids: Int64Array = rows.iter().map(|r| Some(r.0)).collect();
    let tags = tags.finish();
    let scores = scores.finish();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, true),
            Field::new("tags", tags.data_type().clone(), true),
            Field::new("scores", scores.data_type().clone(), true),
        ])),
        vec![Arc::new(ids) as ArrayRef, Arc::new(tags), Arc::new(scores)],
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

fn ints(b: &RecordBatch, column: &str) -> Vec<Option<i64>> {
    let i = b
        .schema()
        .index_of(column)
        .unwrap_or_else(|e| panic!("{e}"));
    b.column(i).as_primitive::<Int64Type>().iter().collect()
}

/// The UNNEST step of a statement's steps, run on the steps before it.
fn unnested(sql: &str, batch: &RecordBatch) -> openetl_sql::Unnested {
    let s = plan_one(sql, &events());
    let op = row_wise(&s);
    let mut current = op.input(batch).unwrap_or_else(|e| panic!("{e}"));
    for step in &op.steps {
        match step {
            Step::Project(p) => current = p.eval(&current).unwrap_or_else(|e| panic!("{e}")),
            Step::Unnest(u) => return u.eval(&current).unwrap_or_else(|e| panic!("{e}")),
            Step::Filter(_) => panic!("no filter here"),
        }
    }
    panic!("no UNNEST in {sql}")
}

#[test]
fn unnest_gives_a_row_per_element_and_none_for_an_empty_or_null_list() {
    let b = event_batch(&[
        (1, Some(&["a", "b"]), None),
        (2, Some(&[]), None),
        (3, None, None),
        (4, Some(&["c"]), None),
    ]);
    let u = unnested(
        "INSERT INTO t SELECT user_id, unnest(tags) AS tag FROM events",
        &b,
    );
    assert_eq!(u.rows, [0, 0, 3]);
    assert_eq!(u.ordinals, [0, 1, 0]);
    assert_eq!(ints(&u.batch, "user_id"), [Some(1), Some(1), Some(4)]);
    let tag = u.batch.column(1).as_string::<i32>();
    assert_eq!(
        tag.iter().collect::<Vec<_>>(),
        [Some("a"), Some("b"), Some("c")]
    );
}

#[test]
fn unnest_outer_keeps_a_row_of_nulls_for_an_empty_or_null_list() {
    let b = event_batch(&[
        (1, Some(&[]), None),
        (2, None, None),
        (3, Some(&["c"]), None),
    ]);
    let u = unnested(
        "INSERT INTO t SELECT user_id, unnest_outer(tags) AS tag FROM events",
        &b,
    );
    assert_eq!(u.rows, [0, 1, 2]);
    let tag = u.batch.column(1).as_string::<i32>();
    assert_eq!(tag.iter().collect::<Vec<_>>(), [None, None, Some("c")]);
}

#[test]
fn several_lists_give_as_many_rows_as_the_longest_with_nulls_past_the_shorter() {
    let b = event_batch(&[
        (1, Some(&["a", "b", "c"]), Some(&[10])),
        (2, None, Some(&[20, 21])),
        (3, Some(&[]), Some(&[])),
    ]);
    let u = unnested(
        "INSERT INTO t SELECT user_id, unnest(tags) AS tag, unnest(scores) AS score FROM events",
        &b,
    );
    assert_eq!(u.rows, [0, 0, 0, 1, 1]);
    assert_eq!(u.ordinals, [0, 1, 2, 0, 1]);
    let tag = u.batch.column(1).as_string::<i32>();
    assert_eq!(
        tag.iter().collect::<Vec<_>>(),
        [Some("a"), Some("b"), Some("c"), None, None]
    );
    assert_eq!(
        ints(&u.batch, &u.batch.schema().field(2).name().clone()),
        [Some(10), None, None, Some(20), Some(21)]
    );
}

#[test]
fn unnest_reads_a_sliced_batch() {
    let b = event_batch(&[(1, Some(&["a"]), None), (2, Some(&["b", "c"]), None)]).slice(1, 1);
    let u = unnested(
        "INSERT INTO t SELECT user_id, unnest(tags) AS tag FROM events",
        &b,
    );
    assert_eq!(u.rows, [0, 0]);
    let tag = u.batch.column(1).as_string::<i32>();
    assert_eq!(tag.iter().collect::<Vec<_>>(), [Some("b"), Some("c")]);
}

#[test]
fn an_expression_says_which_columns_it_reads() {
    let c = customers(fields());
    let s = plan_one(
        "INSERT INTO x SELECT id, lower(email) || name AS label, 'eu' AS region FROM customers WHERE name IS NOT NULL",
        &c,
    );
    let op = row_wise(&s);
    let reads: Vec<&str> = op
        .reads
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let name = |i: usize| reads[i];
    let mut seen = Vec::new();
    for step in &op.steps {
        match step {
            Step::Filter(p) => seen.push(p.inputs().iter().map(|&i| name(i)).collect::<Vec<_>>()),
            Step::Project(p) => {
                for c in &p.columns {
                    seen.push(c.expr.inputs().iter().map(|&i| name(i)).collect());
                }
            }
            Step::Unnest(_) => {}
        }
    }
    assert_eq!(
        seen,
        [vec!["name"], vec!["id"], vec!["email", "name"], vec![]]
    );
}
