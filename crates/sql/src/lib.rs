//! SQL transforms: `INSERT INTO output SELECT …` statements, and the schemas
//! of the streams they read, to a plan of OpenETL's own, whose expressions
//! evaluate on Arrow batches.
//!
//! sqlparser parses; DataFusion binds, coerces types, optimizes and evaluates
//! expressions. None of its types are in this crate's API, and nothing it
//! plans is kept: a plan is made again from the SQL text and the streams'
//! schemas.

mod bind;
mod expr;
mod lower;
mod window;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_sql::sqlparser::ast::{self as ast, Spanned as _};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;

pub use expr::Expr;

/// A stream statements can read.
#[derive(Clone, Debug)]
pub struct Stream {
    pub schema: SchemaRef,
    /// The primary key's columns; empty if it has none.
    pub key: Vec<String>,
    /// Rows are inserts, updates and deletes of their key.
    pub changelog: bool,
}

/// The streams statements can read, by name.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    streams: BTreeMap<String, Stream>,
}

impl Catalog {
    pub fn insert(&mut self, name: impl Into<String>, stream: Stream) {
        self.streams.insert(name.into(), stream);
    }

    pub fn with(mut self, name: impl Into<String>, stream: Stream) -> Self {
        self.insert(name, stream);
        self
    }

    pub fn get(&self, name: &str) -> Option<&Stream> {
        self.streams.get(name)
    }
}

/// A place in the SQL text. Lines and columns count from 1; 0 is unknown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    pub line: u64,
    pub column: u64,
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The text isn't SQL.
    Syntax,
    /// A name or a type doesn't resolve against the catalog.
    Bind,
    /// Valid SQL that transforms don't run.
    Refused,
    /// Evaluating on a batch failed.
    Eval,
}

#[derive(Clone, Debug, thiserror::Error)]
pub struct Error {
    pub kind: ErrorKind,
    pub at: Option<Position>,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.at {
            Some(at) => write!(f, "{at}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl Error {
    fn new(kind: ErrorKind, at: Option<Position>, message: impl Into<String>) -> Self {
        Error {
            kind,
            at,
            message: message.into(),
        }
    }

    pub(crate) fn refused(at: Position, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Refused, Some(at), message)
    }

    pub(crate) fn eval(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Eval, None, message)
    }

    /// A DataFusion error, at its own position if it has one, or else at the
    /// statement's.
    pub(crate) fn datafusion(
        kind: ErrorKind,
        at: Position,
        e: datafusion_common::DataFusionError,
    ) -> Self {
        let at = e
            .diagnostic()
            .and_then(|d| d.span)
            .filter(|s| s.start.line > 0)
            .map_or(at, |s| Position {
                line: s.start.line,
                column: s.start.column,
            });
        let message = match e.diagnostic() {
            Some(d) => d.message.clone(),
            None => e.strip_backtrace(),
        };
        Self::new(kind, Some(at), message)
    }
}

/// What a statement is, which decides where it runs and what state it keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Projections and filters: at most one row out per row in, and no state.
    RowWise,
    /// An aggregate per window of event time and group keys.
    GroupWindow,
}

/// One statement, planned.
#[derive(Debug)]
pub struct Statement {
    /// The stream it writes.
    pub output: String,
    pub at: Position,
    pub class: Class,
    /// The output's.
    pub schema: SchemaRef,
    /// The output's primary key; empty if it has none, and then it can only be
    /// appended.
    pub key: Vec<String>,
    pub operator: Operator,
}

#[derive(Debug)]
pub enum Operator {
    RowWise(RowWise),
    GroupWindow(GroupWindow),
}

/// Steps on one input, a batch at a time.
#[derive(Debug)]
pub struct RowWise {
    pub input: String,
    /// The input's columns it reads, in the order its expressions see them.
    pub reads: SchemaRef,
    pub steps: Vec<Step>,
    /// What it gives.
    pub schema: SchemaRef,
}

#[derive(Debug)]
pub enum Step {
    /// Keeps the rows where the expression is true.
    Filter(Expr),
    Project {
        columns: Vec<Column>,
        schema: SchemaRef,
    },
}

/// A named expression.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub expr: Expr,
}

impl RowWise {
    /// Evaluates the steps on a batch of the input. The batch needs the
    /// columns in `reads`, by name and type, in any order. Its columns named
    /// in `carry` follow the outputs unchanged, filtered with them.
    pub fn eval(&self, batch: &RecordBatch, carry: &[&str]) -> Result<RecordBatch, Error> {
        let schema = batch.schema();
        let mut columns = Vec::with_capacity(self.reads.fields().len());
        for f in self.reads.fields() {
            let i = schema
                .index_of(f.name())
                .map_err(|_| Error::eval(format!("the input has no column {}", f.name())))?;
            if schema.field(i).data_type() != f.data_type() {
                return Err(Error::eval(format!(
                    "column {} is {}, not {}: bind again",
                    f.name(),
                    schema.field(i).data_type(),
                    f.data_type()
                )));
            }
            columns.push(batch.column(i).clone());
        }
        let mut carried = Vec::with_capacity(carry.len());
        for name in carry {
            let i = schema
                .index_of(name)
                .map_err(|_| Error::eval(format!("the input has no column {name}")))?;
            carried.push((schema.field(i).clone(), batch.column(i).clone()));
        }
        let mut current = RecordBatch::try_new(self.reads.clone(), columns)
            .map_err(|e| Error::eval(e.to_string()))?;
        for step in &self.steps {
            match step {
                Step::Filter(predicate) => {
                    let mask = predicate.eval_mask(&current)?;
                    if mask.true_count() == current.num_rows() {
                        continue;
                    }
                    current = arrow_select::filter::filter_record_batch(&current, &mask)
                        .map_err(|e| Error::eval(e.to_string()))?;
                    for (_, c) in &mut carried {
                        *c = arrow_select::filter::filter(c, &mask)
                            .map_err(|e| Error::eval(e.to_string()))?;
                    }
                }
                Step::Project { columns, schema } => {
                    let mut arrays = Vec::with_capacity(columns.len());
                    for c in columns {
                        arrays.push(c.expr.eval(&current)?);
                    }
                    current = RecordBatch::try_new(schema.clone(), arrays)
                        .map_err(|e| Error::eval(e.to_string()))?;
                }
            }
        }
        if carried.is_empty() {
            return Ok(current);
        }
        let mut fields: Vec<_> = current.schema().fields().iter().cloned().collect();
        let mut arrays = current.columns().to_vec();
        for (f, c) in carried {
            fields.push(Arc::new(f));
            arrays.push(c);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| Error::eval(e.to_string()))
    }
}

/// An aggregate per tumbling window and group keys.
#[derive(Debug)]
pub struct GroupWindow {
    /// Row-wise steps before grouping; its schema is what the window, keys
    /// and aggregates read.
    pub pre: RowWise,
    pub window: Window,
    pub keys: Vec<Column>,
    pub aggregates: Vec<Aggregate>,
    /// From a row of the window's start, the keys' values and the aggregates'
    /// values, in that order, to the output.
    pub post: Vec<Step>,
}

#[derive(Debug)]
pub enum Window {
    Tumble {
        /// Evaluates to each row's window start.
        start: Expr,
        /// Microseconds.
        width: i64,
    },
}

#[derive(Debug)]
pub struct Aggregate {
    pub name: String,
    /// `count`, `sum`, `min`, `max`, `avg`, …: DataFusion's names.
    pub function: String,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub data_type: arrow_schema::DataType,
}

/// Statements, parsed once and planned against a catalog as often as the
/// streams' schemas change.
#[derive(Debug)]
pub struct Sql {
    statements: Vec<Parsed>,
}

#[derive(Debug)]
struct Parsed {
    output: String,
    at: Position,
    /// Windows rewritten for binding.
    query: ast::Query,
    inputs: Vec<String>,
}

impl Sql {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, text).map_err(|e| {
            Error::new(
                ErrorKind::Syntax,
                None,
                e.to_string().replace("sql parser error: ", ""),
            )
        })?;
        let mut statements = Vec::with_capacity(parsed.len());
        for s in parsed {
            let start = s.span().start;
            let at = Position {
                line: start.line,
                column: start.column,
            };
            let ast::Statement::Insert(insert) = s else {
                return Err(Error::refused(
                    at,
                    "a transform is INSERT INTO … SELECT statements",
                ));
            };
            let output = match &insert.table {
                ast::TableObject::TableName(n) => object_name(n),
                _ => return Err(Error::refused(at, "INSERT INTO names a stream")),
            };
            if !insert.columns.is_empty()
                || insert.overwrite
                || insert.on.is_some()
                || insert.returning.is_some()
                || insert.partitioned.is_some()
            {
                return Err(Error::refused(
                    at,
                    "INSERT INTO takes a stream's name and a SELECT, and nothing else",
                ));
            }
            let Some(mut query) = insert.source.map(|q| *q) else {
                return Err(Error::refused(at, "INSERT INTO needs a SELECT"));
            };
            window::rewrite(&mut query)?;
            let inputs = inputs(&query);
            statements.push(Parsed {
                output,
                at,
                query,
                inputs,
            });
        }
        let mut seen = std::collections::HashSet::new();
        for s in &statements {
            if !seen.insert(s.output.as_str()) {
                return Err(Error::refused(
                    s.at,
                    format!("{} is written by more than one statement", s.output),
                ));
            }
        }
        Ok(Sql { statements })
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    /// The stream statement `i` writes.
    pub fn output(&self, i: usize) -> &str {
        &self.statements[i].output
    }

    /// The streams statement `i` reads.
    pub fn inputs(&self, i: usize) -> &[String] {
        &self.statements[i].inputs
    }

    /// Plans every statement.
    pub fn plan(&self, catalog: &Catalog) -> Result<Vec<Statement>, Error> {
        (0..self.statements.len())
            .map(|i| self.bind(i, catalog))
            .collect()
    }

    /// Plans statement `i`, which needs only the streams it reads.
    pub fn bind(&self, i: usize, catalog: &Catalog) -> Result<Statement, Error> {
        let s = &self.statements[i];
        let plan = bind::plan(&s.query, catalog, s.at)?;
        lower::statement(&s.output, s.at, &plan, catalog)
    }
}

fn object_name(n: &ast::ObjectName) -> String {
    n.0.iter()
        .filter_map(|p| p.as_ident())
        .map(|i| match i.quote_style {
            Some(_) => i.value.clone(),
            None => i.value.to_lowercase(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The streams a query names in FROM, its joins and subqueries, but not its
/// own WITH's names.
fn inputs(query: &ast::Query) -> Vec<String> {
    use std::ops::ControlFlow;
    let ctes: Vec<String> = query
        .with
        .iter()
        .flat_map(|w| &w.cte_tables)
        .map(|c| c.alias.name.value.to_lowercase())
        .collect();
    let mut names = Vec::new();
    let _ = ast::visit_relations(query, |r: &ast::ObjectName| {
        let n = object_name(r);
        if !names.contains(&n) && !ctes.contains(&n) {
            names.push(n);
        }
        ControlFlow::<()>::Continue(())
    });
    names
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, TimestampMicrosecondType};
    use arrow_array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, TimeUnit};

    /// The two statements of the docs' example.
    const SHOP: &str = "
        INSERT INTO customers
        SELECT id, lower(email) AS email, 'eu' AS region, created_at
        FROM customers;

        INSERT INTO revenue_by_minute
        SELECT window_start, region, sum(amount) AS revenue, count(*) AS orders
        FROM orders
        GROUP BY tumble(created_at, INTERVAL '1 minute'), region;
    ";

    fn ts() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    fn shop() -> Catalog {
        let customers = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("created_at", ts(), true),
        ]);
        let orders = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new("created_at", ts(), true),
        ]);
        Catalog::default()
            .with(
                "customers",
                Stream {
                    schema: Arc::new(customers),
                    key: vec!["id".into()],
                    changelog: false,
                },
            )
            .with(
                "orders",
                Stream {
                    schema: Arc::new(orders),
                    key: vec!["id".into()],
                    changelog: false,
                },
            )
    }

    fn fields(s: &SchemaRef) -> Vec<(String, DataType)> {
        s.fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    #[test]
    fn plans_the_examples_two_statements() {
        let sql = Sql::parse(SHOP).unwrap();
        assert_eq!(sql.len(), 2);
        assert_eq!(sql.inputs(1), ["orders"]);
        let plan = sql.plan(&shop()).unwrap();

        let s = &plan[0];
        assert_eq!((s.output.as_str(), s.class), ("customers", Class::RowWise));
        assert_eq!(s.at, Position { line: 2, column: 9 });
        assert_eq!(
            fields(&s.schema),
            [
                ("id".into(), DataType::Int64),
                ("email".into(), DataType::Utf8),
                ("region".into(), DataType::Utf8),
                ("created_at".into(), ts()),
            ]
        );
        assert_eq!(s.key, ["id"]);
        let Operator::RowWise(op) = &s.operator else {
            panic!("not row-wise")
        };
        // The optimizer reads only what the statement uses.
        assert_eq!(
            op.reads
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "email", "created_at"]
        );

        let s = &plan[1];
        assert_eq!(
            (s.output.as_str(), s.class),
            ("revenue_by_minute", Class::GroupWindow)
        );
        assert_eq!(
            fields(&s.schema),
            [
                ("window_start".into(), ts()),
                ("region".into(), DataType::Utf8),
                ("revenue".into(), DataType::Decimal128(22, 2)),
                ("orders".into(), DataType::Int64),
            ]
        );
        assert_eq!(s.key, ["window_start", "region"]);
        let Operator::GroupWindow(g) = &s.operator else {
            panic!("not a group window")
        };
        let Window::Tumble { width, .. } = &g.window;
        assert_eq!(*width, 60_000_000);
        let aggs: Vec<_> = g.aggregates.iter().map(|a| a.function.as_str()).collect();
        assert_eq!(aggs, ["sum", "count"]);
        assert_eq!(g.keys.len(), 1);
    }

    fn customers(ids: &[i64], emails: &[&str]) -> RecordBatch {
        let n = ids.len();
        let created: Vec<i64> = (0..n as i64).map(|i| 1_700_000_000_000_000 + i).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("created_at", ts(), true),
                Field::new("email", DataType::Utf8, true),
                Field::new("id", DataType::Int64, false),
                Field::new("_openetl_op", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["n"; n])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(created).with_timezone("UTC")),
                Arc::new(StringArray::from(emails.to_vec())),
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vec!["insert"; n])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn evaluates_a_row_wise_statement_by_column_name() {
        let sql = Sql::parse(SHOP).unwrap();
        let s = sql.bind(0, &shop()).unwrap();
        let Operator::RowWise(op) = &s.operator else {
            panic!("not row-wise")
        };
        let out = op
            .eval(&customers(&[1, 2], &["A@X.io", "b@Y.io"]), &["_openetl_op"])
            .unwrap();
        let names: Vec<_> = out
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            ["id", "email", "region", "created_at", "_openetl_op"]
        );
        let email: Vec<_> = out.column(1).as_string::<i32>().iter().flatten().collect();
        assert_eq!(email, ["a@x.io", "b@y.io"]);
        let region: Vec<_> = out.column(2).as_string::<i32>().iter().flatten().collect();
        assert_eq!(region, ["eu", "eu"]);
    }

    #[test]
    fn a_filter_drops_rows_and_their_carried_columns() {
        let sql = Sql::parse(
            "INSERT INTO customers SELECT id, email FROM customers WHERE email LIKE '%@x.io'",
        )
        .unwrap();
        let s = sql.bind(0, &shop()).unwrap();
        let Operator::RowWise(op) = &s.operator else {
            panic!("not row-wise")
        };
        let out = op
            .eval(
                &customers(&[1, 2, 3], &["a@x.io", "b@y.io", "c@x.io"]),
                &["_openetl_op"],
            )
            .unwrap();
        let ids: Vec<_> = out
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(ids, [1, 3]);
        assert_eq!(out.column(2).len(), 2);
    }

    #[test]
    fn tumble_gives_each_rows_window_start() {
        let sql = Sql::parse(SHOP).unwrap();
        let s = sql.bind(1, &shop()).unwrap();
        let Operator::GroupWindow(g) = &s.operator else {
            panic!("not a group window")
        };
        let Window::Tumble { start, .. } = &g.window;
        let minute = 60_000_000;
        let times = TimestampMicrosecondArray::from(vec![minute + 5, 2 * minute - 1, -1])
            .with_timezone("UTC");
        let batch = RecordBatch::try_new(
            g.pre.schema.clone(),
            g.pre
                .schema
                .fields()
                .iter()
                .map(|f| match f.name().as_str() {
                    "created_at" => Arc::new(times.clone()) as ArrayRef,
                    _ => arrow_array::new_null_array(f.data_type(), 3),
                })
                .collect(),
        )
        .unwrap();
        let starts: Vec<_> = start
            .eval(&batch)
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(starts, [minute, minute, -minute]);
    }

    #[test]
    fn binds_hashes_regexes_and_time_functions() {
        let sql = Sql::parse(
            "INSERT INTO customers SELECT id, md5(email) AS h, sha256(email) AS s,
             regexp_replace(email, '@.*', '') AS u, date_trunc('minute', created_at) AS m,
             CASE WHEN id > 1 THEN 'big' ELSE 'small' END AS size, CAST(id AS VARCHAR) AS t
             FROM customers",
        )
        .unwrap();
        let s = sql.bind(0, &shop()).unwrap();
        assert_eq!(s.key, ["id"]);
        assert_eq!(
            s.schema.field_with_name("t").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    /// A view inlined into its reader is a WITH, which DataFusion plans as a
    /// subquery alias: one row-wise statement, keyed through both.
    #[test]
    fn a_with_lowers_to_one_row_wise_statement() {
        let sql = Sql::parse(
            "INSERT INTO active
             WITH cleaned AS (SELECT id, lower(email) AS email FROM customers WHERE id > 1)
             SELECT id AS customer, email FROM cleaned WHERE email LIKE '%@x.io'",
        )
        .unwrap();
        assert_eq!(sql.inputs(0), ["customers"]);
        let s = sql.bind(0, &shop()).unwrap();
        assert_eq!(s.class, Class::RowWise);
        assert_eq!(s.key, ["customer"]);
        let Operator::RowWise(op) = &s.operator else {
            panic!("not row-wise")
        };
        let out = op
            .eval(&customers(&[1, 2, 3], &["a@x.io", "B@X.io", "c@y.io"]), &[])
            .unwrap();
        let email: Vec<_> = out.column(1).as_string::<i32>().iter().flatten().collect();
        assert_eq!(email, ["b@x.io"]);
    }

    fn refused(sql: &str) -> Error {
        let parsed = match Sql::parse(sql) {
            Ok(p) => p,
            Err(e) => return e,
        };
        parsed.plan(&shop()).unwrap_err()
    }

    #[test]
    fn refuses_what_transforms_dont_run_with_its_position() {
        let e = refused("INSERT INTO x SELECT id FROM customers ORDER BY id");
        assert_eq!(e.kind, ErrorKind::Refused);
        assert!(e.message.contains("ORDER BY"), "{e}");

        let e = refused("INSERT INTO x SELECT region, count(*) FROM orders GROUP BY region");
        assert_eq!(e.kind, ErrorKind::Refused);
        assert!(e.message.contains("without a window"), "{e}");

        let e = refused("SELECT 1");
        assert_eq!(e.kind, ErrorKind::Refused);

        let e = refused("INSERT INTO x\nSELECT id, nope FROM customers");
        assert_eq!(e.kind, ErrorKind::Bind);
        assert_eq!(
            e.at,
            Some(Position {
                line: 2,
                column: 12
            }),
            "{e}"
        );

        let e = refused(
            "INSERT INTO x SELECT window_start, count(*) FROM orders \
             GROUP BY hop(created_at, INTERVAL '1 minute', INTERVAL '5 minutes')",
        );
        assert!(e.message.contains("hop"), "{e}");

        let e = refused(
            "INSERT INTO x SELECT * FROM customers JOIN orders ON customers.id = orders.id",
        );
        assert!(e.message.contains("join"), "{e}");
    }

    #[test]
    fn a_changelog_keeps_its_key() {
        let mut c = shop();
        let mut s = c.get("customers").unwrap().clone();
        s.changelog = true;
        c.insert("customers", s);
        let sql =
            Sql::parse("INSERT INTO customers SELECT id + 1 AS id, email FROM customers").unwrap();
        let e = sql.plan(&c).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Refused);
        let sql = Sql::parse("INSERT INTO customers SELECT id AS customer, email FROM customers")
            .unwrap();
        assert_eq!(sql.plan(&c).unwrap()[0].key, ["customer"]);
    }
}
