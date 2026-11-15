//! What a statement passes through unchanged, and what it needs of its
//! input: what keeps a partitioning across it, and whether an exchange may
//! sit before it.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use openetl_sql::{Catalog, Stream};

fn catalog() -> Catalog {
    let list = DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)));
    let mut c = Catalog::new();
    c.insert(
        "orders",
        Stream {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("customer_id", DataType::Int64, true),
                Field::new("email", DataType::Utf8, true),
                Field::new("tags", list, true),
            ])),
            key: vec!["id".into()],
            changelog: false,
            event_time: None,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    c
}

fn passes(sql: &str) -> Vec<(String, String)> {
    let plan = openetl_sql::plan(sql, &catalog()).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let [s] = plan.statements.as_slice() else {
        panic!("{sql}: {} statements", plan.statements.len())
    };
    assert_eq!(s.need(), None, "{sql}: a row-wise statement needs nothing");
    s.passes()
}

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

#[test]
fn columns_pass_renamed_or_not() {
    assert_eq!(
        passes(
            "INSERT INTO o SELECT id AS order_id, lower(email) AS email, customer_id FROM orders WHERE email IS NOT NULL"
        ),
        pairs(&[("id", "order_id"), ("customer_id", "customer_id")])
    );
}

#[test]
fn a_column_passed_twice_passes_under_its_first_name() {
    assert_eq!(
        passes("INSERT INTO o SELECT id AS a, id AS b FROM orders"),
        pairs(&[("id", "a")])
    );
}

/// A cast, even to the column's own type, is computed.
#[test]
fn a_computed_column_does_not_pass() {
    assert_eq!(
        passes("INSERT INTO o SELECT id + 1 AS id, CAST(customer_id AS BIGINT) AS c FROM orders"),
        pairs(&[])
    );
}

/// UNNEST repeats a row's other values on each row it gives, so they pass,
/// though the key doesn't survive it.
#[test]
fn unnest_passes_what_it_repeats() {
    assert_eq!(
        passes("INSERT INTO o SELECT id, unnest(tags) AS tag FROM orders"),
        pairs(&[("id", "id")])
    );
}

#[test]
fn views_pass_what_their_statements_pass() {
    let plan = openetl_sql::plan(
        "CREATE VIEW v AS SELECT id AS order_id, customer_id FROM orders;\n\
         INSERT INTO o SELECT order_id AS oid FROM v",
        &catalog(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(plan.statements[0].passes(), pairs(&[("id", "oid")]));
}

/// A group window needs its group keys, as the stream names them, and
/// passes them, as the output names them; without keys it needs every row
/// on one lane.
#[test]
fn a_group_window_needs_its_keys_and_passes_them() {
    let mut c = catalog();
    c.insert(
        "events",
        Stream {
            schema: Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Int64, true),
                Field::new("kind", DataType::Utf8, true),
                Field::new(
                    "at",
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ])),
            key: Vec::new(),
            changelog: false,
            event_time: Some("at".into()),
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let one = |sql: &str| {
        let mut plan = openetl_sql::plan(sql, &c).unwrap_or_else(|e| panic!("{sql}: {e}"));
        plan.statements.remove(0)
    };
    let s = one(
        "INSERT INTO o SELECT window_start, user_id AS uid, kind, count(*) AS n FROM events \
         GROUP BY tumble(at, INTERVAL '1 hour'), user_id, kind",
    );
    assert_eq!(
        s.need(),
        Some(&["user_id".to_owned(), "kind".to_owned()][..])
    );
    assert_eq!(s.passes(), pairs(&[("user_id", "uid"), ("kind", "kind")]));
    assert_eq!(s.key, ["window_start", "uid", "kind"]);
    assert!(s.operator_id().is_some_and(|id| id.starts_with("window-")));
    let s = one(
        "INSERT INTO o SELECT window_start, count(*) AS n FROM events GROUP BY session(at, INTERVAL '1 minute')",
    );
    assert_eq!(s.need(), Some(&[][..]));
    assert!(s.passes().is_empty());
}

/// A group window's operator is another where it reads another stream, or
/// its rows go through other steps first, so it never restores another's
/// state.
#[test]
fn a_group_windows_operator_is_its_inputs() {
    let mut c = Catalog::new();
    for name in ["a", "b"] {
        c.insert(
            name,
            Stream {
                schema: Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Utf8, true),
                    Field::new("n", DataType::Int64, true),
                    Field::new(
                        "at",
                        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                        true,
                    ),
                ])),
                key: Vec::new(),
                changelog: false,
                event_time: Some("at".into()),
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }
    let id = |from: &str, filter: &str| {
        let sql = format!(
            "INSERT INTO o SELECT window_start, k, sum(n) AS total FROM {from} AS s {filter} \
             GROUP BY tumble(at, INTERVAL '1 minute'), k"
        );
        let plan = openetl_sql::plan(&sql, &c).unwrap_or_else(|e| panic!("{sql}: {e}"));
        plan.statements[0].operator_id().map(str::to_owned)
    };
    assert_eq!(id("a", ""), id("a", ""));
    assert_ne!(id("a", ""), id("b", ""));
    assert_ne!(id("a", ""), id("a", "WHERE n > 0"));
}
