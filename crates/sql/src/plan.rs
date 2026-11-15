use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType, SchemaRef};

use crate::{Error, Expr, Note, Position};

/// What a statement is, which decides where it runs and what state it keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Class {
    /// Projections, scalar functions, casts, `CASE`, `WHERE` and `UNNEST`:
    /// no state, in every lane, before any exchange.
    RowWise,
    /// `GROUP BY tumble/hop/session(…)` with aggregates.
    GroupWindow,
    /// `… OVER (PARTITION BY …)`.
    Over,
    /// Joins, and subqueries that read another stream.
    Join,
    /// Aggregates without a window, and `DISTINCT`: they change rows they
    /// have already given.
    Updating,
}

impl Class {
    /// The class's name, which doesn't change.
    pub fn code(&self) -> &'static str {
        match self {
            Class::RowWise => "row_wise",
            Class::GroupWindow => "group_window",
            Class::Over => "over",
            Class::Join => "join",
            Class::Updating => "updating",
        }
    }

    /// What the class runs, for messages.
    pub(crate) fn description(&self) -> &'static str {
        match self {
            Class::RowWise => "row-wise statements",
            Class::GroupWindow => "group windows",
            Class::Over => "window functions",
            Class::Join => "joins",
            Class::Updating => "updating statements",
        }
    }

    /// The SQL surface's tier that runs it.
    pub fn tier(&self) -> u8 {
        match self {
            Class::RowWise => 1,
            Class::GroupWindow => 2,
            Class::Over => 3,
            Class::Join => 4,
            Class::Updating => 5,
        }
    }
}

/// A transform's SQL, planned against the streams it reads.
#[derive(Debug)]
pub struct Plan {
    /// The `INSERT INTO` statements, in the order they're written.
    pub statements: Vec<Statement>,
    /// The views, in the order they're defined. They're inlined into the
    /// statements that read them, so they run nowhere of their own.
    pub views: Vec<View>,
    pub notes: Vec<Note>,
}

/// One `INSERT INTO … SELECT`, planned.
#[derive(Debug)]
pub struct Statement {
    /// Where its `INSERT` is.
    pub at: Position,
    /// The stream it writes: a source stream's whole name if it replaces one,
    /// or else a name of its own.
    pub output: String,
    /// It writes a source stream's name, so its output lands in that stream's
    /// place.
    pub replaces: bool,
    pub class: Class,
    /// The source streams it reads, directly or through views.
    pub inputs: Vec<String>,
    /// The views it reads, directly or through other views, in the order
    /// they're defined.
    pub views: Vec<String>,
    /// The output's.
    pub schema: SchemaRef,
    /// The output's primary key: the input's key, column for column, where
    /// each of its columns passes through unchanged, renamed or not. Empty
    /// otherwise, and then the output can only be appended.
    pub key: Vec<String>,
    pub operator: Operator,
}

impl Statement {
    /// What its operator needs of its input: the columns whose equal values
    /// must be on one lane, which an exchange by key group gives where the
    /// input's partitioning doesn't; `None` for nothing, as a row-wise
    /// operator, which runs in the lane that read its rows. A group window
    /// needs its group keys, by the stream's names, and keeps its state by
    /// their key groups; with none, it needs every row on one lane.
    pub fn need(&self) -> Option<&[String]> {
        match &self.operator {
            Operator::RowWise(_) => None,
            Operator::GroupWindow(g) => Some(&g.need),
        }
    }

    /// The input's columns that reach the output unchanged, each with its
    /// name there: a partitioning by them is the output's too.
    pub fn passes(&self) -> Vec<(String, String)> {
        match &self.operator {
            Operator::RowWise(r) => r.passes(),
            Operator::GroupWindow(g) => g.passes.clone(),
        }
    }

    /// Its stateful operator's ID; `None` for a statement without state.
    pub fn operator_id(&self) -> Option<&str> {
        match &self.operator {
            Operator::RowWise(_) => None,
            Operator::GroupWindow(g) => Some(&g.id),
        }
    }
}

/// A `CREATE VIEW`, planned on its own.
#[derive(Debug)]
pub struct View {
    pub name: String,
    /// Where its name is.
    pub at: Position,
    /// The source streams it reads, directly or through other views.
    pub inputs: Vec<String>,
    pub schema: SchemaRef,
    /// Its key, by the rule a statement's follows.
    pub key: Vec<String>,
}

#[derive(Debug)]
pub enum Operator {
    RowWise(RowWise),
    GroupWindow(Box<GroupWindow>),
}

/// `GROUP BY` a window over the stream's event time, and keys, with
/// aggregates. Each row goes to the windows its event time falls in, and
/// each window's row is given once the watermark passes its end. Times
/// are in the event-time column's unit: microseconds, or nanoseconds
/// where the column keeps them.
#[derive(Debug)]
pub struct GroupWindow {
    /// The row-wise steps on the stream's batches before the window, whose
    /// output is the rows it groups: [`GroupWindow::rows`].
    pub input: RowWise,
    pub rows: SchemaRef,
    pub window: Window,
    /// The column of `rows` with each row's event time: the stream's
    /// event-time column, passed through.
    pub time: usize,
    pub keys: Vec<GroupKey>,
    pub aggregates: Vec<Aggregate>,
    /// What the steps after the window take: `window_start` and
    /// `window_end`, then each key's column, then each aggregate's value.
    pub grouped: SchemaRef,
    /// On the grouped rows, in order: `HAVING`'s filter, and the
    /// projection to the output.
    pub steps: Vec<Step>,
    /// The stream's columns its keys are, in order.
    pub need: Vec<String>,
    /// The stream's columns that reach the output unchanged, with their
    /// names there: its keys.
    pub passes: Vec<(String, String)>,
    /// Stable while the state's meaning is: a hash of the output, the
    /// window, the keys, the aggregates and the state's layout.
    pub id: String,
}

/// A group key: a column of the rows grouped, which is the stream's
/// column `stream`, renamed or not.
#[derive(Debug)]
pub struct GroupKey {
    pub column: usize,
    pub stream: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Window {
    /// Windows of `size`, one after another from the Unix epoch.
    Tumble { size: i64 },
    /// Windows of `size` that start every `slide`, from the Unix epoch: a
    /// row is in `size / slide` of them, rounded up.
    Hop { slide: i64, size: i64 },
    /// A key's rows with no more than `gap` between one and the next. A
    /// session ends `gap` after its last row.
    Session { gap: i64 },
}

impl Window {
    pub fn name(&self) -> &'static str {
        match self {
            Window::Tumble { .. } => "tumble",
            Window::Hop { .. } => "hop",
            Window::Session { .. } => "session",
        }
    }
}

#[derive(Debug)]
pub struct Aggregate {
    pub kind: AggregateKind,
    /// Its argument, on the rows grouped; `None` for `count(*)`.
    pub arg: Option<Expr>,
    /// `FILTER (WHERE …)`: only rows where it's true count.
    pub filter: Option<Expr>,
    /// Its value's.
    pub data_type: DataType,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggregateKind {
    /// Rows where the argument isn't null; with none, every row.
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// An estimate of how many distinct values the argument has: exact up
    /// to a few hundred, then within about 2%.
    ApproxDistinct,
    /// The value at this fraction of the argument's sorted values,
    /// interpolated between the two nearest: `median` is 0.5.
    Percentile(f64),
}

impl AggregateKind {
    pub fn name(&self) -> &'static str {
        match self {
            AggregateKind::Count => "count",
            AggregateKind::Sum => "sum",
            AggregateKind::Min => "min",
            AggregateKind::Max => "max",
            AggregateKind::Avg => "avg",
            AggregateKind::ApproxDistinct => "approx_distinct",
            AggregateKind::Percentile(_) => "percentile_cont",
        }
    }
}

/// Steps on one source stream's batches, one batch at a time. On a
/// changelog stream, deletes pass without the filters.
#[derive(Clone, Debug)]
pub struct RowWise {
    /// The source stream it reads.
    pub input: String,
    /// Whether that stream is a changelog. The statement then keeps the
    /// stream's key, so a row that fails a filter can become a delete of it.
    pub changelog: bool,
    /// The input's columns it reads, in the order the first step sees them.
    pub reads: SchemaRef,
    /// In the order they run. Each takes the batch the one before gave.
    pub steps: Vec<Step>,
}

impl RowWise {
    /// The input's columns that reach the output unchanged, renamed or not,
    /// each with the first name the output gives it, in the output's order:
    /// through filters, projections that pass them, and `UNNEST`, which
    /// repeats a row's values on each row it gives.
    pub fn passes(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        for (name, from) in self.origins() {
            if let Some(from) = from
                && !out.iter().any(|(f, _)| *f == from)
            {
                out.push((from, name));
            }
        }
        out
    }

    /// The input's column that the output's column `name` holds unchanged,
    /// if it's one.
    pub fn origin(&self, name: &str) -> Option<String> {
        self.origins()
            .into_iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, from)| from)
    }

    /// Each of the output's columns, with the input's column it holds
    /// unchanged, if it's one.
    fn origins(&self) -> Vec<(String, Option<String>)> {
        // Each column of a step's batch, with the input column it holds.
        let mut cols: Vec<(String, Option<String>)> = self
            .reads
            .fields()
            .iter()
            .map(|f| (f.name().clone(), Some(f.name().clone())))
            .collect();
        for step in &self.steps {
            cols = match step {
                Step::Filter(_) => continue,
                Step::Project(p) => p
                    .columns
                    .iter()
                    .map(|c| {
                        let from = c.expr.column().and_then(|n| {
                            cols.iter()
                                .find(|(m, _)| m == n)
                                .and_then(|(_, i)| i.clone())
                        });
                        (c.name.clone(), from)
                    })
                    .collect(),
                Step::Unnest(u) => u
                    .sources
                    .iter()
                    .enumerate()
                    .map(|(j, &src)| {
                        let from = match u.lists.contains(&src) {
                            true => None,
                            false => cols.get(src).and_then(|(_, i)| i.clone()),
                        };
                        (u.schema.field(j).name().clone(), from)
                    })
                    .collect(),
            };
        }
        cols
    }

    /// A batch of the input's columns that the first step reads, from a
    /// batch of the stream that has them, by name and type, in any order.
    pub fn input(&self, batch: &RecordBatch) -> Result<RecordBatch, Error> {
        let schema = batch.schema();
        let mut columns = Vec::with_capacity(self.reads.fields().len());
        for f in self.reads.fields() {
            let Ok(i) = schema.index_of(f.name()) else {
                return Err(Error::eval(format!("the input has no column {}", f.name())));
            };
            if schema.field(i).data_type() != f.data_type() {
                return Err(Error::eval(format!(
                    "column {} is {}, not {}: the statement needs binding again",
                    f.name(),
                    schema.field(i).data_type(),
                    f.data_type()
                )));
            }
            columns.push(batch.column(i).clone());
        }
        batch_of(self.reads.clone(), columns, batch.num_rows())
    }
}

#[derive(Clone, Debug)]
pub enum Step {
    /// Keeps the rows where the predicate is true.
    Filter(Expr),
    Project(Projection),
    Unnest(Unnest),
}

#[derive(Clone, Debug)]
pub struct Projection {
    pub columns: Vec<Column>,
    /// What it gives.
    pub schema: SchemaRef,
}

impl Projection {
    pub fn eval(&self, batch: &RecordBatch) -> Result<RecordBatch, Error> {
        let mut arrays = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            arrays.push(c.expr.eval(batch)?);
        }
        batch_of(self.schema.clone(), arrays, batch.num_rows())
    }
}

/// A named expression.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub expr: Expr,
}

/// Each row's lists flattened into a row per element. With several lists, a
/// row gives as many rows as its longest list, and the shorter ones give
/// nulls past their ends.
#[derive(Clone, Debug)]
pub struct Unnest {
    /// The input's columns that are lists to flatten, by index.
    pub lists: Vec<usize>,
    /// For each output column, the input column it comes from: an element of
    /// that list if it's in `lists`, or else the row's value, repeated.
    pub sources: Vec<usize>,
    /// A null or empty list gives a row of nulls, as `unnest_outer` asks.
    /// Otherwise it gives no rows.
    pub outer: bool,
    /// What it gives.
    pub schema: SchemaRef,
}

/// What [`Unnest::eval`] gives.
#[derive(Debug)]
pub struct Unnested {
    pub batch: RecordBatch,
    /// Each row's row of the input.
    pub rows: Vec<usize>,
    /// Each row's place among its input row's: the index into its lists.
    pub ordinals: Vec<usize>,
}

/// A list column's elements, and where each row's start and how many it has.
struct Elements {
    values: ArrayRef,
    starts: Vec<usize>,
    lens: Vec<usize>,
}

impl Elements {
    fn of(a: &ArrayRef) -> Result<Self, Error> {
        let n = a.len();
        let (values, mut starts, mut lens) = match a.data_type() {
            DataType::List(_) => {
                let l = a.as_list::<i32>();
                let o = l.offsets();
                let (s, n): (Vec<usize>, Vec<usize>) = o
                    .windows(2)
                    .map(|w| {
                        (
                            w[0].unsigned_abs() as usize,
                            (w[1] - w[0]).unsigned_abs() as usize,
                        )
                    })
                    .unzip();
                (l.values().clone(), s, n)
            }
            DataType::LargeList(_) => {
                let l = a.as_list::<i64>();
                let o = l.offsets();
                let mut s = Vec::with_capacity(n);
                let mut k = Vec::with_capacity(n);
                for w in o.windows(2) {
                    s.push(usize::try_from(w[0]).map_err(|e| Error::eval(e.to_string()))?);
                    k.push(usize::try_from(w[1] - w[0]).map_err(|e| Error::eval(e.to_string()))?);
                }
                (l.values().clone(), s, k)
            }
            DataType::FixedSizeList(_, size) => {
                let l = a.as_fixed_size_list();
                let size = usize::try_from(*size).map_err(|e| Error::eval(e.to_string()))?;
                (
                    l.values().clone(),
                    (0..n)
                        .map(|r| l.value_offset(r).unsigned_abs() as usize)
                        .collect(),
                    vec![size; n],
                )
            }
            other => {
                return Err(Error::eval(format!(
                    "UNNEST of {other}, which isn't a list"
                )));
            }
        };
        for r in 0..n {
            if a.is_null(r) {
                lens[r] = 0;
                starts[r] = 0;
            }
        }
        Ok(Self {
            values,
            starts,
            lens,
        })
    }
}

impl Unnest {
    /// Flattens a batch of its input.
    pub fn eval(&self, batch: &RecordBatch) -> Result<Unnested, Error> {
        let n = batch.num_rows();
        let mut lists = Vec::with_capacity(self.lists.len());
        for &i in &self.lists {
            let Some(a) = batch.columns().get(i) else {
                return Err(Error::eval(format!(
                    "UNNEST of column {i}, which the input lacks"
                )));
            };
            lists.push((i, Elements::of(a)?));
        }
        let mut rows = Vec::with_capacity(n);
        let mut ordinals = Vec::with_capacity(n);
        for r in 0..n {
            let most = lists.iter().map(|(_, l)| l.lens[r]).max().unwrap_or(0);
            if most == 0 && self.outer {
                rows.push(r);
                ordinals.push(0);
            }
            for k in 0..most {
                rows.push(r);
                ordinals.push(k);
            }
        }
        let take = |a: &dyn Array, indices: &UInt64Array| {
            arrow_select::take::take(a, indices, None).map_err(|e| Error::eval(e.to_string()))
        };
        let repeated = UInt64Array::from_iter_values(rows.iter().map(|&r| r as u64));
        let mut columns = Vec::with_capacity(self.sources.len());
        for (j, &src) in self.sources.iter().enumerate() {
            let column = match lists.iter().find(|(i, _)| *i == src) {
                Some((_, l)) => {
                    let indices: UInt64Array = rows
                        .iter()
                        .zip(&ordinals)
                        .map(|(&r, &k)| (k < l.lens[r]).then(|| (l.starts[r] + k) as u64))
                        .collect();
                    take(l.values.as_ref(), &indices)?
                }
                None => {
                    let Some(a) = batch.columns().get(src) else {
                        return Err(Error::eval(format!(
                            "UNNEST repeats column {src}, which the input lacks"
                        )));
                    };
                    take(a.as_ref(), &repeated)?
                }
            };
            let want = self.schema.field(j).data_type();
            columns.push(match column.data_type() == want {
                true => column,
                false => {
                    return Err(Error::eval(format!(
                        "UNNEST gave {} for column {}, which is {want}",
                        column.data_type(),
                        self.schema.field(j).name()
                    )));
                }
            });
        }
        let batch = batch_of(self.schema.clone(), columns, rows.len())?;
        Ok(Unnested {
            batch,
            rows,
            ordinals,
        })
    }
}

fn batch_of(
    schema: SchemaRef,
    columns: Vec<arrow_array::ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, Error> {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| Error::eval(e.to_string()))
}
