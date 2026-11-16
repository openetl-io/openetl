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
    // A long expression, written in the statement or in a view, whole or
    // in part.
    let long = vec!["qty"; 60].join(" + ");
    let inline = id(&format!(
        "INSERT INTO revenue SELECT window_start, region, sum({long} + 1) AS total \
         FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
    ));
    assert!(long.len() > 256, "the expression is shorter than the bound");
    for (what, view, arg) in [
        ("the argument", format!("{long} + 1"), "x"),
        ("part of it", long.clone(), "x + 1"),
    ] {
        assert_eq!(
            id(&format!(
                "CREATE VIEW v AS SELECT {view} AS x, region, created_at FROM orders;\n\
                 INSERT INTO revenue SELECT window_start, region, sum({arg}) AS total \
                 FROM v GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
            )),
            inline,
            "a long expression with {what} in a view"
        );
    }
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
                Field::new("xs", list.clone(), true),
                Field::new("ys", list, true),
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
    // Two lists unnested together give a row per element of the longer;
    // one after the other, a row per pair.
    let together =
        "CREATE VIEW a AS SELECT unnest(xs) AS x, unnest(ys) AS y, region, at FROM events;\n";
    let apart = "CREATE VIEW a AS SELECT unnest(xs) AS x, ys, region, at FROM events;\n\
                 CREATE VIEW b AS SELECT x, unnest(ys) AS y, region, at FROM a;\n";
    assert_ne!(count(together, "a"), count(apart, "b"));
    let swapped =
        "CREATE VIEW a AS SELECT unnest(ys) AS y, unnest(xs) AS x, region, at FROM events;\n";
    assert_eq!(count(together, "a"), count(swapped, "a"));
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

/// Values of two unnest steps of one list are two values, whatever names
/// they go by.
#[test]
fn values_of_two_unnest_steps_are_apart() {
    let c = lists();
    let views = "CREATE VIEW a AS SELECT unnest(xs) AS x, xs, region, at FROM events;\n\
                 CREATE VIEW b AS SELECT x, unnest(xs) AS y, region, at FROM a;\n";
    let sum = |of: &str| {
        id_in(
            &c,
            &format!(
                "{views}INSERT INTO n SELECT window_start, region, sum({of}) AS total \
                 FROM b WHERE x < y GROUP BY tumble(at, INTERVAL '1 minute'), region"
            ),
            "n",
        )
    };
    assert_ne!(sum("x"), sum("y"));
}

/// Each call of a function that gives another value each time is another
/// value; a column it computes is one value, however often it's read.
#[test]
fn each_call_of_random_is_its_own() {
    let filtered = |views: &str, from: &str, filter: &str| {
        id(&format!(
            "{views}INSERT INTO revenue SELECT window_start, region, sum(amount) AS total \
             FROM {from} WHERE {filter} GROUP BY tumble(created_at, INTERVAL '1 minute'), region"
        ))
    };
    let once = filtered("", "orders", "random() < 0.5");
    let twice = filtered("", "orders", "random() < 0.5 AND random() < 0.5");
    assert_ne!(once, twice, "two calls filter as two");
    let view = "CREATE VIEW v AS SELECT random() AS r, region, amount, created_at FROM orders;\n";
    assert_eq!(
        filtered(view, "v", "r < 0.5"),
        filtered(view, "v", "r < 0.5 AND r < 0.5"),
        "one value read twice filters as once"
    );
    assert_ne!(filtered(view, "v", "r < 0.5 AND r < 0.5"), twice);
    assert_eq!(
        filtered("", "orders", "qty > 0"),
        filtered("", "orders", "qty > 0 AND qty > 0")
    );
}

/// A call before an unnest gives one value for every element of a row's
/// list, and one after it a value for each element: two computations.
#[test]
fn a_call_before_an_unnest_is_not_one_after_it() {
    let c = lists();
    let count = |views: &str| {
        id_in(
            &c,
            &format!(
                "{views}INSERT INTO n SELECT window_start, region, count(*) AS n \
                 FROM b WHERE r < 0.5 GROUP BY tumble(at, INTERVAL '1 minute'), region"
            ),
            "n",
        )
    };
    let before = "CREATE VIEW a AS SELECT random() AS r, xs, region, at FROM events;\n\
                  CREATE VIEW b AS SELECT r, unnest(xs) AS x, region, at FROM a;\n";
    let after = "CREATE VIEW a AS SELECT unnest(xs) AS x, region, at FROM events;\n\
                 CREATE VIEW b AS SELECT random() AS r, x, region, at FROM a;\n";
    assert_ne!(count(before), count(after));
}

/// A small generator: xorshift64*.
struct Gen(u64);

impl Gen {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
    }

    fn coin(&mut self) -> bool {
        self.below(2) == 0
    }

    fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i + 1);
            v.swap(i, j);
        }
    }
}

const KEYS: [&[&str]; 3] = [&["region"], &["country"], &["region", "country"]];
/// An aggregate and the column it reads; none for `count(*)`.
const AGGREGATES: [(&str, Option<&str>); 4] = [
    ("sum", Some("amount")),
    ("count", None),
    ("max", Some("qty")),
    ("min", Some("note")),
];
/// A filter: the column it reads, and the rest of it.
const FILTERS: [(&str, &str); 2] = [("qty", "> 0"), ("note", "IS NOT NULL")];

/// What a statement computes and keeps.
#[derive(Clone, Debug, PartialEq)]
struct Meaning {
    /// 0: tumble of a minute, 1: of two, 2: hop of two every minute.
    window: usize,
    keys: usize,
    aggregates: Vec<usize>,
    filter: Option<usize>,
}

fn meaning(g: &mut Gen) -> Meaning {
    let mut aggregates: Vec<usize> = (0..AGGREGATES.len()).filter(|_| g.coin()).collect();
    if aggregates.is_empty() {
        aggregates.push(g.below(AGGREGATES.len()));
    }
    Meaning {
        window: g.below(3),
        keys: g.below(KEYS.len()),
        aggregates,
        filter: match g.below(3) {
            0 => None,
            i => Some(i - 1),
        },
    }
}

/// The statement writing revenue that computes m, written one of many
/// ways that don't change what it computes: keyword case, a FROM alias,
/// column names, the order of SELECT and of GROUP BY, an equal interval,
/// and the filter in the statement or in a view.
fn written(g: &mut Gen, m: &Meaning) -> String {
    let kw = |g: &mut Gen, k: &str| match g.coin() {
        true => k.to_uppercase(),
        false => k.to_lowercase(),
    };
    let alias = g.coin().then_some("o");
    let q = |c: &str| match alias {
        Some(a) => format!("{a}.{c}"),
        None => c.to_owned(),
    };
    let time = q("created_at");
    let window = match m.window {
        0 => format!(
            "tumble({time}, INTERVAL '{}')",
            ["1 minute", "60 seconds"][g.below(2)]
        ),
        1 => format!(
            "tumble({time}, INTERVAL '{}')",
            ["2 minutes", "120 seconds"][g.below(2)]
        ),
        _ => format!("hop({time}, INTERVAL '1 minute', INTERVAL '2 minutes')"),
    };
    let mut select = vec![["window_start AS ws", "window_start"][g.below(2)].to_owned()];
    for k in KEYS[m.keys] {
        select.push(match g.coin() {
            true => format!("{} AS k_{k}", q(k)),
            false => q(k),
        });
    }
    for (i, &a) in m.aggregates.iter().enumerate() {
        let (f, col) = AGGREGATES[a];
        let arg = col.map_or("*".to_owned(), q);
        let n = g.below(100);
        let as_ = kw(g, "as");
        select.push(format!("{f}({arg}) {as_} a{i}_{n}"));
    }
    g.shuffle(&mut select);
    let mut group = vec![window];
    group.extend(KEYS[m.keys].iter().map(|k| q(k)));
    g.shuffle(&mut group);
    let in_view = g.coin();
    let (view, from, filter) = match (m.filter.map(|f| FILTERS[f]), in_view) {
        (Some((c, rest)), true) => (
            format!("CREATE VIEW src AS SELECT * FROM orders WHERE {c} {rest};\n"),
            "src",
            String::new(),
        ),
        (Some((c, rest)), false) => (
            String::new(),
            "orders",
            format!(" {} {} {rest}", kw(g, "where"), q(c)),
        ),
        (None, true) => (
            "CREATE VIEW src AS SELECT * FROM orders;\n".to_owned(),
            "src",
            String::new(),
        ),
        (None, false) => (String::new(), "orders", String::new()),
    };
    let from = match alias {
        Some(a) => format!("{from} {a}"),
        None => from.to_owned(),
    };
    format!(
        "{view}{} revenue\n{} {}\n{} {from}{filter} {} {}",
        kw(g, "INSERT INTO"),
        kw(g, "SELECT"),
        select.join(", "),
        kw(g, "FROM"),
        kw(g, "GROUP BY"),
        group.join(", ")
    )
}

/// Random pairs of statements: two that compute and keep the same get one
/// ID however they're written, and two that differ in any way get two.
#[test]
fn ids_follow_what_statements_compute() {
    let mut g = Gen(0x05ee_d1d5);
    let (mut same, mut apart) = (0, 0);
    for _ in 0..120 {
        let a = meaning(&mut g);
        let b = if g.coin() { a.clone() } else { meaning(&mut g) };
        let (sa, sb) = (written(&mut g, &a), written(&mut g, &b));
        let (ia, ib) = (id(&sa), id(&sb));
        if a == b {
            same += 1;
            assert_eq!(ia, ib, "{a:?} written two ways:\n{sa}\n---\n{sb}");
        } else {
            apart += 1;
            assert_ne!(ia, ib, "{a:?} and {b:?} share an ID:\n{sa}\n---\n{sb}");
        }
    }
    assert!(same > 20 && apart > 20, "{same} pairs alike, {apart} apart");
}

const OVER: &str = "INSERT INTO running SELECT id, region, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
    lag(qty) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders";

fn over_id(sql: &str) -> String {
    id_in(&catalog(DataType::Utf8), sql, "running")
}

#[test]
fn an_over_operators_id_keeps_through_edits_that_keep_what_it_computes() {
    let base = over_id(OVER);
    assert!(base.starts_with("over-"), "{base}");
    let same = [
        (
            "formatting, keyword case, an alias and other names",
            "insert into running\n  select o.id, o.region AS r, LAG(o.qty) over (partition BY o.region order by o.created_at) as b,\n  \
             SUM(o.amount) OVER (PARTITION BY o.region ORDER BY o.created_at) AS t FROM source.orders o",
        ),
        (
            "the default frame written out",
            "INSERT INTO running SELECT id, region, sum(amount) OVER (PARTITION BY region ORDER BY created_at \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total, \
             lag(qty, 1) OVER (PARTITION BY region ORDER BY created_at ROWS UNBOUNDED PRECEDING) AS before FROM orders",
        ),
        (
            "another column passed through, and an expression over the values",
            "INSERT INTO running SELECT id, region, note, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
             lag(qty) OVER (PARTITION BY region ORDER BY created_at) + 1 AS before FROM orders",
        ),
        (
            "a filter after it",
            "INSERT INTO running SELECT * FROM (SELECT id, region, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
             lag(qty) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders) AS s WHERE total > 0",
        ),
        (
            "a view that renames its columns",
            "CREATE VIEW o AS SELECT id, region AS r, amount AS a, qty, created_at AS t FROM orders;\n\
             INSERT INTO running SELECT id, r, sum(a) OVER (PARTITION BY r ORDER BY t) AS total, \
             lag(qty) OVER (PARTITION BY r ORDER BY t) AS before FROM o",
        ),
    ];
    for (what, sql) in same {
        assert_eq!(over_id(sql), base, "{what}: {sql}");
    }
}

#[test]
fn an_over_operators_id_changes_with_what_it_computes_or_keeps() {
    let base = over_id(OVER);
    let other = [
        (
            "another key",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY country ORDER BY created_at) AS total, \
             lag(qty) OVER (PARTITION BY country ORDER BY created_at) AS before FROM orders",
        ),
        (
            "a key more",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region, country ORDER BY created_at) AS total, \
             lag(qty) OVER (PARTITION BY country, region ORDER BY created_at) AS before FROM orders",
        ),
        (
            "another frame",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at ROWS UNBOUNDED PRECEDING) AS total, \
             lag(qty) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders",
        ),
        (
            "a bounded frame",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS total, \
             lag(qty) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders",
        ),
        (
            "another offset",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
             lag(qty, 2) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders",
        ),
        (
            "a default",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
             lag(qty, 1, 0) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders",
        ),
        (
            "a function less",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total FROM orders",
        ),
        (
            "another function",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
             first_value(qty) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders",
        ),
        (
            "a filter before it",
            "INSERT INTO running SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS total, \
             lag(qty) OVER (PARTITION BY region ORDER BY created_at) AS before FROM orders WHERE qty > 0",
        ),
    ];
    let mut seen = vec![base];
    for (what, sql) in other {
        let got = over_id(sql);
        assert!(
            !seen.contains(&got),
            "{what}: {sql} gave {got}, as another did"
        );
        seen.push(got);
    }
    assert_eq!(seen.len(), 10, "every edit was compared");
}

const DEDUPE: &str = "INSERT INTO latest SELECT id, region, amount FROM orders \
    QUALIFY row_number() OVER (PARTITION BY region, id ORDER BY created_at DESC) = 1";

fn dedupe_id(sql: &str) -> String {
    id_in(&catalog(DataType::Utf8), sql, "latest")
}

#[test]
fn a_dedupes_id_keeps_through_edits_that_keep_what_it_keeps() {
    let base = dedupe_id(DEDUPE);
    assert!(base.starts_with("dedupe-"), "{base}");
    let same = [
        (
            "a subquery for QUALIFY, keys in another order and other names",
            "INSERT INTO latest SELECT id AS i, region AS r, amount, n FROM (\
             SELECT *, ROW_NUMBER() OVER (PARTITION BY id, region ORDER BY created_at DESC) AS n FROM orders) AS t WHERE n = 1",
        ),
        (
            "another column passed through, and a filter after it",
            "INSERT INTO latest SELECT id, region, amount, note FROM orders \
             QUALIFY row_number() OVER (PARTITION BY region, id ORDER BY created_at DESC) = 1 AND amount > 0",
        ),
    ];
    for (what, sql) in same {
        assert_eq!(dedupe_id(sql), base, "{what}: {sql}");
    }
    let other = [
        (
            "the first row, not the latest",
            "INSERT INTO latest SELECT id, region, amount FROM orders \
             QUALIFY row_number() OVER (PARTITION BY region, id ORDER BY created_at) = 1",
        ),
        (
            "a key less",
            "INSERT INTO latest SELECT id, region, amount FROM orders \
             QUALIFY row_number() OVER (PARTITION BY id ORDER BY created_at DESC) = 1",
        ),
        (
            "a filter before it",
            "INSERT INTO latest SELECT id, region, amount FROM (SELECT *, row_number() OVER \
             (PARTITION BY region, id ORDER BY created_at DESC) AS n FROM orders WHERE qty > 0) AS t WHERE n = 1",
        ),
    ];
    let mut seen = vec![base];
    for (what, sql) in other {
        let got = dedupe_id(sql);
        assert!(
            !seen.contains(&got),
            "{what}: {sql} gave {got}, as another did"
        );
        seen.push(got);
    }
}

/// An `OVER` operator's keys and functions are in an order of their own;
/// a dedupe's output is keyed by its partition keys as `PARTITION BY`
/// writes them, and says whether it upserts.
#[test]
fn over_and_dedupe_take_an_order_of_their_own() {
    let c = catalog(DataType::Utf8);
    let plan = openetl_sql::plan(
        "INSERT INTO running SELECT id, max(qty) OVER (PARTITION BY region, country ORDER BY created_at) AS most, \
         count(*) OVER (PARTITION BY country, region ORDER BY created_at) AS n FROM orders",
        &c,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let s = &plan.statements[0];
    let Operator::Over(o) = &s.operator else {
        panic!("not OVER: {:?}", s.operator)
    };
    assert_eq!(o.need, ["country", "region"]);
    assert_eq!(
        o.computes,
        ["count(*) range unbounded", "max(qty) range unbounded"]
    );
    assert_eq!(s.key, ["id"]);
    assert_eq!(s.need().map(<[String]>::to_vec), Some(o.need.clone()));

    let plan = openetl_sql::plan(DEDUPE, &c).unwrap_or_else(|e| panic!("{e}"));
    let s = &plan.statements[0];
    let Operator::Dedupe(d) = &s.operator else {
        panic!("not a dedupe: {:?}", s.operator)
    };
    assert_eq!(d.need, ["id", "region"]);
    assert_eq!(s.key, ["region", "id"]);
    assert!(d.keep.upserts());
    assert_eq!(s.class, openetl_sql::Class::Over);
}
