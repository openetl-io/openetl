//! Runs the cases in `tests/plan/*.txt`: each plans SQL against the streams
//! declared before it and compares what planning gave, plan or errors, with
//! the text the case expects.
//!
//! A file holds directives separated by blank lines:
//!
//! ```text
//! # A comment.
//!
//! stream public.orders key(id) changelog
//!   id int64 not null
//!   status utf8
//!
//! plan
//! INSERT INTO orders SELECT id, upper(status) AS status FROM public.orders
//! ----
//! statement 1:1 public.orders replaces row_wise
//! …
//! ```
//!
//! `stream` adds a stream to the catalog the cases after it plan against,
//! with a column per indented line; a name with spaces or quotes is written
//! in double quotes, and a `json` column is a string the engine marks as
//! JSON, as a schema shows with `(json)`. `reset` empties the catalog.
//! `plan` gives SQL, then `----` and what planning gives. Run with
//! `OPENETL_SQL_REWRITE=1` to write what each case gives into its file.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use openetl_sql::{Catalog, ErrorKind, Errors, Operator, Plan, Step, Stream};

#[test]
fn cases() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/plan");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "txt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no cases in {}", dir.display());
    let rewrite = std::env::var_os("OPENETL_SQL_REWRITE").is_some();
    let mut failed = Vec::new();
    for file in files {
        let text =
            std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        let (out, failures) = run(&text);
        let name = file.file_name().map(|n| n.to_string_lossy().into_owned());
        let name = name.unwrap_or_default();
        if rewrite {
            if out != text {
                std::fs::write(&file, out).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
            }
        } else {
            failed.extend(failures.into_iter().map(|f| format!("{name}: {f}")));
        }
    }
    assert!(
        failed.is_empty(),
        "{} cases differ; OPENETL_SQL_REWRITE=1 writes what they give:\n\n{}",
        failed.len(),
        failed.join("\n\n")
    );
}

/// The file with each case's expected text replaced by what it gives, and
/// the cases where the two differ.
fn run(text: &str) -> (String, Vec<String>) {
    let mut catalog = Catalog::new();
    let mut out = String::new();
    let mut failures = Vec::new();
    let mut line = 1;
    for block in text.split("\n\n") {
        let start = line;
        line += block.matches('\n').count() + 2;
        let trimmed = block.trim_matches('\n');
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        let body: Vec<&str> = trimmed.lines().filter(|l| !l.starts_with('#')).collect();
        let Some(first) = body.first() else {
            out.push_str(trimmed);
            continue;
        };
        if *first == "reset" {
            catalog = Catalog::new();
            out.push_str(trimmed);
        } else if first.starts_with("stream ") {
            stream(&mut catalog, &body).unwrap_or_else(|e| panic!("line {start}: {e}"));
            out.push_str(trimmed);
        } else if *first == "plan" {
            let comments: Vec<&str> = trimmed.lines().take_while(|l| l.starts_with('#')).collect();
            let rest = &body[1..];
            let sep = rest
                .iter()
                .position(|l| *l == "----")
                .unwrap_or_else(|| panic!("line {start}: a plan case without ----"));
            let sql = rest[..sep].join("\n");
            let want = rest[sep + 1..].join("\n");
            let got = match openetl_sql::plan(&sql, &catalog) {
                Ok(p) => render(&p),
                Err(e) => errors(&e),
            };
            if got != want {
                failures.push(format!(
                    "line {start}:\n{sql}\n---- want\n{want}\n---- got\n{got}"
                ));
            }
            for c in comments {
                out.push_str(c);
                out.push('\n');
            }
            let _ = write!(out, "plan\n{sql}\n----\n{got}");
        } else {
            panic!("line {start}: {first:?} isn't a directive");
        }
    }
    if !out.ends_with('\n') {
        out.push('\n');
    }
    (out, failures)
}

/// A `stream` directive: `stream NAME [key(A, B)] [changelog]`, then a line
/// per column: `NAME TYPE [not null]`.
fn stream(catalog: &mut Catalog, lines: &[&str]) -> Result<(), String> {
    let head = lines[0].strip_prefix("stream ").unwrap_or_default();
    let (name, rest) = word(head);
    let mut key = Vec::new();
    let mut changelog = false;
    for w in split_outside_parens(rest) {
        if let Some(k) = w.strip_prefix("key(").and_then(|k| k.strip_suffix(')')) {
            key = k.split(',').map(|c| c.trim().to_owned()).collect();
        } else if w == "changelog" {
            changelog = true;
        } else {
            return Err(format!("{w:?} in {:?}", lines[0]));
        }
    }
    let mut fields = Vec::new();
    for l in &lines[1..] {
        let l = l.trim();
        let (column, rest) = word(l);
        let (ty, nullable) = match rest.strip_suffix(" not null") {
            Some(t) => (t, false),
            None => (rest, true),
        };
        let field = match ty.trim() {
            "json" => Field::new(column, DataType::Utf8, nullable).with_metadata(json()),
            t => Field::new(column, data_type(t)?, nullable),
        };
        fields.push(field);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    catalog
        .insert(
            name,
            Stream {
                schema,
                key,
                changelog,
            },
        )
        .map_err(|e| e.to_string())
}

/// The first word, or a double-quoted string, and what follows it.
fn word(s: &str) -> (String, &str) {
    let s = s.trim();
    if let Some(q) = s.strip_prefix('"')
        && let Some(end) = q.find('"')
    {
        return (q[..end].to_owned(), q[end + 1..].trim());
    }
    match s.split_once(' ') {
        Some((w, rest)) => (w.to_owned(), rest.trim()),
        None => (s.to_owned(), ""),
    }
}

fn split_outside_parens(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut depth = 0;
    let mut current = String::new();
    for c in s.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ' ' if depth == 0 => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn data_type(t: &str) -> Result<DataType, String> {
    Ok(match t {
        "bool" => DataType::Boolean,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ => {
            if let Some(item) = t.strip_prefix("list<").and_then(|t| t.strip_suffix('>')) {
                DataType::List(Arc::new(Field::new_list_field(data_type(item)?, true)))
            } else if let Some(ps) = t.strip_prefix("decimal(").and_then(|t| t.strip_suffix(')')) {
                let (p, s) = ps.split_once(',').ok_or(t.to_string())?;
                DataType::Decimal128(
                    p.trim().parse().map_err(|_| t.to_owned())?,
                    s.trim().parse().map_err(|_| t.to_owned())?,
                )
            } else {
                return Err(format!("{t} isn't a type these tests know"));
            }
        }
    })
}

/// What marks a string column as JSON, as the engine marks one.
fn json() -> HashMap<String, String> {
    HashMap::from([
        (EXTENSION_NAME.to_owned(), "arrow.json".to_owned()),
        ("ARROW:extension:metadata".to_owned(), String::new()),
    ])
}

const EXTENSION_NAME: &str = "ARROW:extension:name";

/// A column's name and type, and ` (json)` where it holds JSON.
fn schema(s: &SchemaRef) -> String {
    s.fields()
        .iter()
        .map(|f| {
            let null = if f.is_nullable() { "" } else { " not null" };
            let json = match f.metadata().get(EXTENSION_NAME) {
                Some(e) if e == "arrow.json" => " (json)",
                _ => "",
            };
            format!("{} {}{json}{null}", f.name(), f.data_type())
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn names(s: &SchemaRef) -> String {
    s.fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn render(p: &Plan) -> String {
    let mut out = String::new();
    for v in &p.views {
        let _ = writeln!(out, "view {} {}", v.at, v.name);
        let _ = writeln!(out, "  inputs {}", v.inputs.join(", "));
        let _ = writeln!(out, "  key {}", v.key.join(", "));
        let _ = writeln!(out, "  schema {}", schema(&v.schema));
    }
    for s in &p.statements {
        let replaces = if s.replaces { " replaces" } else { "" };
        let _ = writeln!(
            out,
            "statement {} {}{replaces} {}",
            s.at,
            s.output,
            s.class.code()
        );
        let _ = writeln!(out, "  inputs {}", s.inputs.join(", "));
        if !s.views.is_empty() {
            let _ = writeln!(out, "  views {}", s.views.join(", "));
        }
        let _ = writeln!(out, "  key {}", s.key.join(", "));
        let _ = writeln!(out, "  schema {}", schema(&s.schema));
        let Operator::RowWise(op) = &s.operator;
        let changelog = if op.changelog { " changelog" } else { "" };
        let _ = writeln!(out, "  reads {}{changelog}: {}", op.input, names(&op.reads));
        for step in &op.steps {
            match step {
                Step::Filter(e) => {
                    let _ = writeln!(out, "  filter {e}");
                }
                Step::Project(p) => {
                    let columns: Vec<String> = p
                        .columns
                        .iter()
                        .map(|c| match c.expr.column() {
                            Some(from) if from == c.name => c.name.clone(),
                            _ => format!("{} := {}", c.name, c.expr),
                        })
                        .collect();
                    let _ = writeln!(out, "  project {}", columns.join(", "));
                }
                Step::Unnest(u) => {
                    let outer = if u.outer { " outer" } else { "" };
                    let _ = writeln!(
                        out,
                        "  unnest{outer} lists {:?} sources {:?}: {}",
                        u.lists,
                        u.sources,
                        names(&u.schema)
                    );
                }
            }
        }
    }
    for n in &p.notes {
        let _ = writeln!(out, "note {} {}", n.at, n.message);
    }
    out.trim_end().to_owned()
}

fn errors(e: &Errors) -> String {
    e.iter()
        .map(|e| {
            let kind = match e.kind {
                ErrorKind::LaterTier(c) => format!("{} {}", e.kind.code(), c.code()),
                k => k.code().to_owned(),
            };
            match e.at {
                Some(at) => format!("error {kind} {at} {}", e.message),
                None => format!("error {kind} {}", e.message),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}
