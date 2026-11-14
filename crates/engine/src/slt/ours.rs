//! Our engine as sqllogictest's database. A table is a stream and its rows
//! one batch of it. A query is the statement `INSERT INTO slt_result` with
//! it, planned with the views defined before it, and run by the row-wise
//! operator a lane runs. Errors are `kind line:column message`, with the
//! position in the record's own SQL.
//!
//! A changelog table's `INSERT` may set `_op` (insert, update or delete;
//! insert by default) and `_unsent`, the columns an update didn't send. A
//! query on one gives each row's op first, and `unchanged:` with the
//! columns an update leaves unchanged last. A row the statement can't
//! evaluate, which a lane quarantines, is `quarantined row N`, counting
//! the table's rows from 1.

use std::fmt;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use openetl_normalize::{Control, Typed};
use openetl_sql::{Catalog, ErrorKind, Errors, Stream};
use sqllogictest::{DBOutput, DefaultColumnType};

use super::rows::{self, Kind, Lit, Table, Tok};
use crate::stage::Stage;
use crate::transform::{Input, Transform};

/// What a query gave: its rows, each value written by [`rows::cell`], and
/// the rows a lane quarantines, apart from them.
#[derive(Default)]
pub(crate) struct Ran {
    types: Vec<DefaultColumnType>,
    pub rows: Vec<Vec<String>>,
    /// The output, where the record writes several, and the row of its
    /// table, counted from 1.
    pub quarantined: Vec<(Option<String>, usize)>,
}

impl Ran {
    /// As a record's result: the rows, then `quarantined row N` for each
    /// row quarantined.
    fn output(self) -> Output {
        let mut rows = self.rows;
        for (output, r) in self.quarantined {
            let mut row: Vec<String> = output.into_iter().collect();
            row.push("quarantined".into());
            row.push(format!("row {r}"));
            rows.push(row);
        }
        DBOutput::Rows {
            types: self.types,
            rows,
        }
    }
}

/// What a statement the test runs as a query writes.
const RESULT: &str = "slt_result";

#[derive(Debug)]
pub(crate) struct Failure(pub String);

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Failure {}

impl From<String> for Failure {
    fn from(s: String) -> Self {
        Failure(s)
    }
}

#[derive(Default)]
pub(crate) struct Ours {
    tables: Vec<Data>,
    /// The `CREATE VIEW`s so far, as written.
    views: Vec<String>,
}

struct Data {
    table: Table,
    batches: Vec<RecordBatch>,
    ops: Vec<Option<String>>,
    unsent: Vec<Option<Vec<String>>>,
}

pub(crate) type Output = DBOutput<DefaultColumnType>;

impl sqllogictest::DB for Ours {
    type Error = Failure;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<Output, Failure> {
        let sql = statement(sql);
        match rows::kind(sql) {
            Kind::Table => {
                let t = rows::table(sql)?;
                if self.tables.iter().any(|d| d.table.name == t.name) {
                    return Err(Failure(format!("table {} exists", t.name)));
                }
                self.tables.push(Data {
                    table: t,
                    batches: Vec::new(),
                    ops: Vec::new(),
                    unsent: Vec::new(),
                });
                Ok(DBOutput::StatementComplete(0))
            }
            Kind::Values => self.insert(sql),
            Kind::View => {
                // An unread view plans too, so a statement over any stream
                // checks it.
                let check = match self.tables.first() {
                    Some(d) => format!(
                        "INSERT INTO slt_check SELECT * FROM \"{}\"",
                        d.table.name.replace('"', "\"\"")
                    ),
                    None => format!("INSERT INTO slt_check SELECT * FROM {}", view_name(sql)),
                };
                self.plan(&format!("{sql};\n{check}"), false)?;
                self.views.push(sql.to_owned());
                Ok(DBOutput::StatementComplete(0))
            }
            Kind::Query => self.query(sql, true).map(Ran::output),
            Kind::Transform => self.query(sql, false).map(Ran::output),
            Kind::Describe => {
                let body = sql
                    .trim_start()
                    .get("DESCRIBE".len()..)
                    .unwrap_or_default()
                    .trim_start();
                self.describe(body)
            }
        }
    }

    fn engine_name(&self) -> &str {
        "openetl"
    }
}

/// A record's SQL without the `;` that may end it.
pub(crate) fn statement(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim_end()
}

/// A `CREATE VIEW`'s name as written.
fn view_name(sql: &str) -> String {
    let t = rows::tokens(sql).unwrap_or_default();
    let mut name = String::new();
    for tok in t.iter().skip(2) {
        match tok {
            Tok::Word(w) if w.eq_ignore_ascii_case("AS") => break,
            Tok::Word(w) => name.push_str(w),
            Tok::Quoted(q) => name.push_str(&format!("\"{}\"", q.replace('"', "\"\""))),
            Tok::Punct(p) => name.push(*p),
            _ => {}
        }
    }
    name
}

impl Ours {
    /// The table of that name, or else the one whose name differs only by
    /// case.
    fn data(&mut self, name: &str) -> Result<&mut Data, Failure> {
        let i = self
            .tables
            .iter()
            .position(|d| d.table.name == name)
            .or_else(|| {
                self.tables
                    .iter()
                    .position(|d| d.table.name.eq_ignore_ascii_case(name))
            })
            .ok_or_else(|| Failure(format!("there's no table {name}")))?;
        Ok(&mut self.tables[i])
    }

    fn insert(&mut self, sql: &str) -> Result<Output, Failure> {
        let v = rows::values(sql)?;
        let data = self.data(&v.table)?;
        let schema = data.table.schema.clone();
        let columns: Vec<String> = match v.columns {
            Some(c) => c,
            None => schema.fields().iter().map(|f| f.name().clone()).collect(),
        };
        if let Some(r) = v.rows.iter().find(|r| r.len() != columns.len()) {
            return Err(Failure(format!(
                "a row of {} values for {} columns",
                r.len(),
                columns.len()
            )));
        }
        let at = |name: &str| columns.iter().position(|c| c.eq_ignore_ascii_case(name));
        for c in &columns {
            let pseudo = data.table.changelog && (c == "_op" || c == "_unsent");
            if !pseudo
                && schema
                    .fields()
                    .iter()
                    .all(|f| !f.name().eq_ignore_ascii_case(c))
            {
                return Err(Failure(format!("{} has no column {c}", data.table.name)));
            }
        }
        let mut arrays = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let lits: Vec<&Lit> = v
                .rows
                .iter()
                .map(|r| at(f.name()).map_or(&Lit::Null, |j| &r[j]))
                .collect();
            arrays.push(rows::array(f.data_type(), &lits)?);
        }
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| Failure(e.to_string()))?;
        for r in &v.rows {
            data.ops.push(match at("_op").map(|j| &r[j]) {
                Some(Lit::Text(op)) => Some(op.clone()),
                _ => None,
            });
            data.unsent.push(match at("_unsent").map(|j| &r[j]) {
                Some(Lit::List(names)) => Some(
                    names
                        .iter()
                        .filter_map(|n| match n {
                            Lit::Text(n) => Some(n.clone()),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => None,
            });
        }
        data.batches.push(batch);
        Ok(DBOutput::StatementComplete(v.rows.len() as u64))
    }

    fn catalog(&self) -> Result<Catalog, Failure> {
        let mut c = Catalog::new();
        for d in &self.tables {
            c.insert(
                d.table.name.clone(),
                Stream {
                    schema: d.table.schema.clone(),
                    key: d.table.key.clone(),
                    changelog: d.table.changelog,
                },
            )
            .map_err(|e| Failure(e.to_string()))?;
        }
        Ok(c)
    }

    /// The views, then `body`, wrapped as a statement if `wrap`: the SQL to
    /// plan, and how many lines come before `body`.
    fn text(&self, body: &str, wrap: bool) -> (String, u64) {
        let mut text = String::new();
        for v in &self.views {
            text.push_str(v);
            text.push_str(";\n");
        }
        if wrap {
            text.push_str(&format!("INSERT INTO {RESULT}\n"));
        }
        let before = text.matches('\n').count() as u64;
        text.push_str(body);
        (text, before)
    }

    /// The plan, the SQL it's of, and how many lines precede `body` in it.
    fn plan(&self, body: &str, wrap: bool) -> Result<(openetl_sql::Plan, String, u64), Failure> {
        let (text, before) = self.text(body, wrap);
        let plan = openetl_sql::plan(&text, &self.catalog()?)
            .map_err(|e| Failure(errors(&e, before, wrap)))?;
        Ok((plan, text, before))
    }

    fn describe(&self, body: &str) -> Result<Output, Failure> {
        let (plan, _, before) = self.plan(body, true)?;
        let Some(st) = plan.statements.last() else {
            return Err(Failure("nothing to describe".into()));
        };
        let mut rows: Vec<Vec<String>> = st
            .schema
            .fields()
            .iter()
            .map(|f| {
                let mut t = f.data_type().to_string();
                if rows::is_json(f) {
                    t.push_str(" json");
                }
                let null = if f.is_nullable() { "null" } else { "not null" };
                let mut row = vec![f.name().clone(), t, null.into()];
                if st.key.contains(f.name()) {
                    row.push("key".into());
                }
                row
            })
            .collect();
        for n in &plan.notes {
            rows.push(vec![
                "note".into(),
                position(n.at, before, true),
                n.message.clone(),
            ]);
        }
        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text; 3],
            rows,
        })
    }

    /// Runs a query, or a transform's statements as written, on the tables'
    /// rows.
    pub(crate) fn query(&self, body: &str, wrap: bool) -> Result<Ran, Failure> {
        let (_, text, _) = self.plan(body, wrap)?;
        let inputs: Vec<Input> = self
            .tables
            .iter()
            .map(|d| Input {
                name: d.table.name.clone(),
                schema: Some(d.table.schema.clone()),
                key: d.table.key.clone(),
                changelog: d.table.changelog,
            })
            .collect();
        let t = Arc::new(
            Transform::plan(&text, inputs)
                .map_err(|e| Failure(format!("internal {}", e.message)))?,
        );
        let several = t.statements.len() > 1;
        let mut ran = Ran::default();
        for (i, st) in t.statements.iter().enumerate() {
            let data = &self.tables[st.input];
            let typed = data.typed()?;
            let pushed: Vec<usize> = (0..typed.batch.num_rows()).collect();
            let applied = Stage::statement(&t, i)
                .apply(&typed, &pushed, None)
                .map_err(|e| Failure(format!("{:?} {}", e.code, e.message)))?;
            let b = &applied.typed.batch;
            let n = b.num_rows();
            let ops: Vec<Option<String>> = match &applied.typed.control.op {
                Some(o) => o
                    .as_string::<i32>()
                    .iter()
                    .map(|v| v.map(str::to_owned))
                    .collect(),
                None => vec![None; n],
            };
            let unchanged: Vec<Option<ArrayRef>> = match &applied.typed.control.unchanged {
                Some(u) => u.as_list::<i32>().iter().collect(),
                None => vec![None; n],
            };
            if ran.types.is_empty() {
                ran.types = b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| rows::column_type(f.data_type()))
                    .collect();
            }
            for (r, mut values) in rows::rows(b).into_iter().enumerate() {
                let mut row = Vec::with_capacity(values.len() + 3);
                if several {
                    row.push(st.output.clone());
                }
                if data.table.changelog {
                    row.push(ops[r].clone().unwrap_or_else(|| "insert".into()));
                }
                row.append(&mut values);
                if let Some(names) = &unchanged[r] {
                    let names: Vec<&str> = names.as_string::<i32>().iter().flatten().collect();
                    row.push(format!("unchanged:{}", names.join(",")));
                }
                ran.rows.push(row);
            }
            for (r, _) in &applied.failed {
                ran.quarantined
                    .push((several.then(|| st.output.clone()), r + 1));
            }
        }
        Ok(ran)
    }
}

impl Data {
    /// The table's rows as one batch of its stream, with an ID each and,
    /// for a changelog, their ops in order.
    fn typed(&self) -> Result<Typed, Failure> {
        let batch = arrow_select::concat::concat_batches(&self.table.schema, &self.batches)
            .map_err(|e| Failure(e.to_string()))?;
        let n = batch.num_rows();
        let ids = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            (0..n as u64).map(|i| {
                let mut id = [0u8; 16];
                id[..8].copy_from_slice(&i.to_le_bytes());
                Some(id)
            }),
            16,
        )
        .map_err(|e| Failure(e.to_string()))?;
        let control = match self.table.changelog {
            false => Control::default(),
            true => {
                let mut unsent = ListBuilder::new(StringBuilder::new());
                for u in &self.unsent {
                    match u {
                        Some(names) => {
                            for name in names {
                                unsent.values().append_value(name);
                            }
                            unsent.append(true);
                        }
                        None => unsent.append_null(),
                    }
                }
                Control {
                    op: Some(Arc::new(StringArray::from(self.ops.clone())) as ArrayRef),
                    seq: Some(Arc::new(Int64Array::from_iter_values(0..n as i64))),
                    unchanged: Some(Arc::new(unsent.finish())),
                }
            }
        };
        Ok(Typed {
            batch,
            ids,
            control,
            cells: Vec::new(),
            rejected: Vec::new(),
            rows: None,
        })
    }
}

/// Planning's errors, one a line, each at its position in the record's
/// SQL, which `before` lines precede.
fn errors(e: &Errors, before: u64, wrap: bool) -> String {
    e.iter()
        .map(|e| {
            let kind = match e.kind {
                ErrorKind::LaterTier(c) => format!("{} {}", e.kind.code(), c.code()),
                k => k.code().to_owned(),
            };
            match e.at {
                Some(at) => format!("{kind} {} {}", position(at, before, wrap), e.message),
                None => format!("{kind} {}", e.message),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A position in the record's SQL, which `before` lines precede. With
/// `wrap`, the last of them is the `INSERT` around the record's query, and a
/// position on it is the statement's: the record's start.
fn position(at: openetl_sql::Position, before: u64, wrap: bool) -> String {
    match at.line > before {
        true => format!("{}:{}", at.line - before, at.column),
        false if wrap && at.line == before => "1:1".into(),
        false => format!("(before the record, {at})"),
    }
}
