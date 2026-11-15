//! A stateful operator's ID, which names its state in a checkpoint: the same
//! for edits that change neither what the operator computes nor what it
//! keeps, and another for edits that change either.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use openetl_sql::{Catalog, Operator, Stream};

fn stream(region: DataType) -> Stream {
    Stream {
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("region", region, true),
            Field::new("country", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new("qty", DataType::Int32, true),
            Field::new("note", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ])),
        key: vec!["id".into()],
        changelog: false,
        event_time: Some("created_at".into()),
    }
}

fn catalog(region: DataType) -> Catalog {
    let mut c = Catalog::new();
    for name in ["orders", "returns"] {
        c.insert(name, stream(region.clone()))
            .unwrap_or_else(|e| panic!("{e}"));
    }
    c
}

/// The ID of the operator of the statement writing `output`.
fn id_in(c: &Catalog, sql: &str, output: &str) -> String {
    let plan = openetl_sql::plan(sql, c).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let Some(s) = plan.statements.iter().find(|s| s.output == output) else {
        panic!("{sql}: no statement writes {output}")
    };
    s.operator_id()
        .unwrap_or_else(|| panic!("{sql}: no operator"))
        .to_owned()
}

fn id(sql: &str) -> String {
    id_in(&catalog(DataType::Utf8), sql, "revenue")
}

const BASE: &str = "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
    FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region";

const TWO_KEYS: &str = "INSERT INTO revenue SELECT window_start, region, country, sum(amount) AS total \
    FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region, country";

#[test]
fn edits_that_keep_what_it_computes_keep_its_id() {
    let base = id(BASE);
    let same = [
        (
            "formatting, comments, keyword case and an equal interval",
            "insert into revenue\n  -- the revenue\n  select WINDOW_START, Region, SUM(amount) as total, COUNT(*) n\n  \
             from ORDERS /* every order */\n  group by TUMBLE(created_at, interval '60 seconds'), region",
        ),
        (
            "a FROM alias",
            "INSERT INTO revenue SELECT window_start, o.region, sum(o.amount) AS total, count(*) AS n \
             FROM orders AS o GROUP BY tumble(o.created_at, INTERVAL '1 minute'), o.region",
        ),
        (
            "another FROM alias",
            "INSERT INTO revenue SELECT window_start, x.region, sum(x.amount) AS total, count(*) AS n \
             FROM source.orders x GROUP BY tumble(x.created_at, INTERVAL '1 minute'), x.region",
        ),
        (
            "other names for the output's columns",
            "INSERT INTO revenue SELECT window_start AS minute, region AS r, sum(amount) AS revenue, count(*) AS orders \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "the aggregates in another order",
            "INSERT INTO revenue SELECT count(*) AS n, region, window_start, sum(amount) AS total \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "count(1) for count(*)",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(1) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "HAVING, and window_end",
            "INSERT INTO revenue SELECT window_start, window_end, region, sum(amount) AS total, count(*) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region HAVING count(*) > 1",
        ),
        (
            "an expression over the aggregates",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n, sum(amount) / count(*) AS mean \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "a view that renames its columns and passes another",
            "CREATE VIEW o AS SELECT region AS r, amount AS a, created_at, note FROM orders;\n\
             INSERT INTO revenue SELECT window_start, r, sum(a) AS total, count(*) AS n \
             FROM o GROUP BY tumble(created_at, INTERVAL '1 minute'), r",
        ),
        (
            "another statement before it",
            "INSERT INTO ids SELECT id FROM returns;\n\
             INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
    ];
    for (what, sql) in same {
        assert_eq!(id(sql), base, "{what}: {sql}");
    }
    let two = id(TWO_KEYS);
    assert_eq!(
        id(
            "INSERT INTO revenue SELECT window_start, country, region, sum(amount) AS total \
             FROM orders GROUP BY country, tumble(created_at, INTERVAL '1 minute'), region"
        ),
        two,
        "GROUP BY and SELECT in another order"
    );
    let filtered = "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
        FROM orders WHERE qty > 0 AND region <> 'x' GROUP BY tumble(created_at, INTERVAL '1 minute'), region";
    assert_eq!(
        id(filtered),
        id(
            "CREATE VIEW o AS SELECT * FROM orders WHERE region <> 'x';\n\
             INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM o WHERE qty > 0 GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
        ),
        "the same filters, split between a view and the statement"
    );
    assert_eq!(
        id(
            "CREATE VIEW o AS SELECT region, qty AS amount, created_at FROM orders;\n\
             INSERT INTO revenue SELECT window_start, region, sum(amount) AS total \
             FROM o GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
        ),
        id(
            "INSERT INTO revenue SELECT window_start, region, sum(qty) AS total \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
        ),
        "an argument that a view names as another column"
    );
}

#[test]
fn edits_that_change_what_it_computes_or_keeps_change_its_id() {
    let base = id(BASE);
    let other = [
        (
            "another size",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '2 minutes'), region",
        ),
        (
            "another kind of window",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM orders GROUP BY hop(created_at, INTERVAL '1 minute', INTERVAL '2 minutes'), region",
        ),
        (
            "a session",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM orders GROUP BY session(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "another key",
            "INSERT INTO revenue SELECT window_start, country, sum(amount) AS total, count(*) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), country",
        ),
        ("a key more", TWO_KEYS),
        (
            "an aggregate more",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n, max(qty) AS most \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "an aggregate less",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "another aggregate's argument",
            "INSERT INTO revenue SELECT window_start, region, sum(qty) AS total, count(*) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "an expression as the argument",
            "INSERT INTO revenue SELECT window_start, region, sum(amount * 2) AS total, count(*) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "a column's count for every row's",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(note) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "an aggregate's FILTER",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) FILTER (WHERE qty > 1) AS n \
             FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "a filter before the window",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM orders WHERE qty > 0 GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "a filter in a view",
            "CREATE VIEW o AS SELECT * FROM orders WHERE qty > 5;\n\
             INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM o GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "another stream",
            "INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM returns GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
        (
            "an argument computed in a view, under the column's name",
            "CREATE VIEW o AS SELECT region, amount + 1 AS amount, created_at FROM orders;\n\
             INSERT INTO revenue SELECT window_start, region, sum(amount) AS total, count(*) AS n \
             FROM o GROUP BY tumble(created_at, INTERVAL '1 minute'), region",
        ),
    ];
    let mut seen = vec![base.clone()];
    for (what, sql) in other {
        let got = id(sql);
        assert!(
            !seen.contains(&got),
            "{what}: {sql} gave {got}, as another did"
        );
        seen.push(got);
    }
    assert_eq!(seen.len(), 16, "every edit was compared");
    let renamed = BASE.replace("INTO revenue", "INTO revenue_by_minute");
    assert_ne!(
        id_in(&catalog(DataType::Utf8), &renamed, "revenue_by_minute"),
        base,
        "another output"
    );
    assert_ne!(
        id_in(&catalog(DataType::Int64), BASE, "revenue"),
        base,
        "a key of another type"
    );
}

/// The keys and aggregates of the operator are in an order of their own,
/// so its key groups and its state's layout follow what it computes. The
/// output's key follows GROUP BY.
#[test]
fn keys_and_aggregates_take_an_order_of_their_own() {
    let c = catalog(DataType::Utf8);
    let sql = "INSERT INTO revenue SELECT window_start, region, country, max(qty) AS most, sum(amount) AS total \
        FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region, country";
    let plan = openetl_sql::plan(sql, &c).unwrap_or_else(|e| panic!("{e}"));
    let s = &plan.statements[0];
    let Operator::GroupWindow(g) = &s.operator else {
        panic!("not a group window")
    };
    assert_eq!(g.need, ["country", "region"]);
    assert_eq!(g.computes, ["max(qty)", "sum(amount)"]);
    assert_eq!(g.event_time, "created_at");
    assert_eq!(s.key, ["window_start", "region", "country"]);
}

fn lists() -> Catalog {
    let list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
    let mut c = Catalog::new();
    c.insert(
        "events",
        Stream {
            schema: Arc::new(Schema::new(vec![
                Field::new("xs", list, true),
                Field::new("unnest(xs)", DataType::Int64, true),
                Field::new("region", DataType::Utf8, true),
                Field::new(
                    "at",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ])),
            key: Vec::new(),
            changelog: false,
            event_time: Some("at".into()),
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    c
}

/// An unnested value is never written as a stream's column, whatever the
/// column's name.
#[test]
fn an_unnest_is_not_a_column_of_its_name() {
    let c = lists();
    let view =
        "CREATE VIEW v AS SELECT unnest(xs) AS x, \"unnest(xs)\" AS y, region, at FROM events;\n";
    let sum = |of: &str| {
        id_in(
            &c,
            &format!(
                "{view}INSERT INTO revenue SELECT window_start, region, sum({of}) AS total \
                 FROM v GROUP BY tumble(at, INTERVAL '1 minute'), region"
            ),
            "revenue",
        )
    };
    assert_ne!(sum("x"), sum("y"));
}

/// Each unnest multiplies the rows, so unnesting a list twice is another
/// operator than unnesting it once.
#[test]
fn each_unnest_counts() {
    let c = lists();
    let once = "CREATE VIEW a AS SELECT unnest(xs) AS x, xs, region, at FROM events;\n";
    let twice = "CREATE VIEW a AS SELECT unnest(xs) AS x, xs, region, at FROM events;\n\
                 CREATE VIEW b AS SELECT unnest(xs) AS y, region, at FROM a;\n";
    let count = |views: &str, from: &str| {
        id_in(
            &c,
            &format!(
                "{views}INSERT INTO n SELECT window_start, region, count(*) AS n \
                 FROM {from} GROUP BY tumble(at, INTERVAL '1 minute'), region"
            ),
            "n",
        )
    };
    assert_ne!(count(once, "a"), count(twice, "b"));
}

/// The deepest chain of views, each reading a column of the one before
/// twice, plans in time that grows with the chain, and its operator's ID
/// still follows what it computes. Planning has a stack of its own.
#[test]
fn a_deep_chain_of_views_keeps_its_id_short() {
    // v0 and the views after it, up to v{depth}: depth + 1 views.
    let chain = |depth: usize, last: &str| {
        let mut sql =
            String::from("CREATE VIEW v0 AS SELECT qty AS x, region, created_at FROM orders;\n");
        for i in 1..=depth {
            let op = if i == depth { last } else { "+" };
            sql.push_str(&format!(
                "CREATE VIEW v{i} AS SELECT x {op} x AS x, region, created_at FROM v{};\n",
                i - 1
            ));
        }
        sql.push_str(&format!(
            "INSERT INTO revenue SELECT window_start, region, sum(x) AS total \
             FROM v{depth} GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
        ));
        sql
    };
    let deepest = openetl_sql::VIEW_DEPTH - 1;
    let start = std::time::Instant::now();
    let deep = id(&chain(deepest, "+"));
    assert!(
        start.elapsed() < std::time::Duration::from_secs(20),
        "{} views took {:?}",
        deepest + 1,
        start.elapsed()
    );
    assert_eq!(deep, id(&chain(deepest, "+")));
    assert_ne!(deep, id(&chain(deepest, "*")));
    assert_ne!(deep, id(&chain(deepest - 1, "+")));
    let too_deep = chain(deepest + 1, "+");
    let Err(e) = openetl_sql::plan(&too_deep, &catalog(DataType::Utf8)) else {
        panic!("{} views planned", deepest + 2)
    };
    assert!(e.to_string().contains("views go at most 32 deep"), "{e}");
}
