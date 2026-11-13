use std::fmt;

use datafusion_common::{DataFusionError, SchemaError};
use datafusion_sql::sqlparser::tokenizer::Span;

use crate::plan::Class;

/// A place in the SQL text. Lines and columns count from 1; a column counts
/// characters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub line: u64,
    pub column: u64,
}

impl Position {
    /// Where a span starts, or `None` for a span the parser didn't fill.
    pub(crate) fn of(span: Span) -> Option<Position> {
        (span.start.line > 0).then_some(Position {
            line: span.start.line,
            column: span.start.column,
        })
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

/// What went wrong, as a stable kind a caller can match on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The text isn't SQL.
    Syntax,
    /// SQL that transforms don't run: statements other than `INSERT INTO`
    /// and `CREATE VIEW`, and clauses such as `ORDER BY`, `LIMIT` and `WITH`.
    Unsupported,
    /// SQL of a class that a later tier runs.
    LaterTier(Class),
    /// A stream, view or output's name doesn't resolve, fits more than one
    /// stream, takes the wrong qualifier, or is taken.
    Name,
    /// A column, function or type doesn't bind.
    Bind,
    /// A function that isn't deterministic feeds a key or a window.
    NotDeterministic,
    /// A statement or view on a changelog stream drops or changes a column of
    /// its key.
    ChangelogKey,
    /// The catalog itself is wrong: a stream named twice, or a key column
    /// its schema doesn't have.
    Catalog,
    /// Evaluating an expression on a batch failed.
    Eval,
    /// A fault of ours, not of the SQL.
    Internal,
}

impl ErrorKind {
    /// The kind's name, which doesn't change.
    pub fn code(&self) -> &'static str {
        match self {
            ErrorKind::Syntax => "syntax",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::LaterTier(_) => "later_tier",
            ErrorKind::Name => "name",
            ErrorKind::Bind => "bind",
            ErrorKind::NotDeterministic => "not_deterministic",
            ErrorKind::ChangelogKey => "changelog_key",
            ErrorKind::Catalog => "catalog",
            ErrorKind::Eval => "eval",
            ErrorKind::Internal => "internal",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub struct Error {
    pub kind: ErrorKind,
    /// Where in the SQL text; `None` for errors that aren't about the text,
    /// such as a catalog's or evaluation's.
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
    pub(crate) fn new(kind: ErrorKind, at: Option<Position>, message: impl Into<String>) -> Self {
        Error {
            kind,
            at,
            message: message.into(),
        }
    }

    pub(crate) fn at(kind: ErrorKind, at: Position, message: impl Into<String>) -> Self {
        Self::new(kind, Some(at), message)
    }

    pub(crate) fn eval(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Eval, None, message)
    }

    /// A DataFusion error, at its own position if it has one, or else at
    /// `at`. Its internal errors are ours.
    pub(crate) fn datafusion(kind: ErrorKind, at: Position, e: &DataFusionError) -> Self {
        let root = e.find_root();
        if matches!(root, DataFusionError::Internal(_)) {
            return Self::at(
                ErrorKind::Internal,
                at,
                format!(
                    "planning failed inside the SQL engine: {}",
                    root.message().trim_end()
                ),
            );
        }
        let diagnostic = e.diagnostic();
        let at = diagnostic
            .and_then(|d| d.span)
            .filter(|s| s.start.line > 0)
            .map_or(at, |s| Position {
                line: s.start.line,
                column: s.start.column,
            });
        let missing = match root {
            DataFusionError::SchemaError(e, _) => match e.as_ref() {
                SchemaError::FieldNotFound {
                    field,
                    valid_fields,
                } => Some(not_found(field, valid_fields)),
                _ => None,
            },
            _ => None,
        };
        let message = match (diagnostic, missing) {
            (_, Some(m)) => m,
            (Some(d), None) => match d.message.strip_prefix("Invalid function '") {
                Some(name) => format!(
                    "{} isn't a function transforms can call",
                    name.trim_end_matches('\'')
                ),
                None if d.notes.is_empty() => d.message.clone(),
                None => format!(
                    "{} ({})",
                    d.message,
                    d.notes
                        .iter()
                        .map(|n| n.message.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            },
            (None, None) => root.message().into_owned(),
        };
        Self::at(kind, at, message)
    }
}

/// A column that isn't there, with the columns that are.
fn not_found(field: &datafusion_common::Column, valid: &[datafusion_common::Column]) -> String {
    let name = &field.name;
    if let Some(other) = valid.iter().find(|c| c.name == *name) {
        return format!("there's no column {field}; there's {other}");
    }
    if let Some(other) = valid.iter().find(|c| c.name.eq_ignore_ascii_case(name)) {
        return format!(
            "there's no column {name}: names fold to lower case unless quoted, so write \"{}\"",
            other.name
        );
    }
    let mut names: Vec<&str> = Vec::new();
    for c in valid {
        if !names.contains(&c.name.as_str()) {
            names.push(&c.name);
        }
    }
    format!("there's no column {name}; there are {}", names.join(", "))
}

/// The errors of planning a transform's SQL, in the order of their positions.
/// Never empty.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub struct Errors(Vec<Error>);

impl Errors {
    pub(crate) fn new(mut errors: Vec<Error>) -> Self {
        errors.sort_by_key(|e| e.at);
        errors.dedup();
        Errors(errors)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Error> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_vec(self) -> Vec<Error> {
        self.0
    }
}

impl From<Error> for Errors {
    fn from(e: Error) -> Self {
        Errors(vec![e])
    }
}

impl IntoIterator for Errors {
    type Item = Error;
    type IntoIter = std::vec::IntoIter<Error>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Errors {
    type Item = &'a Error;
    type IntoIter = std::slice::Iter<'a, Error>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{e}")?;
        }
        Ok(())
    }
}

/// Something `openetl plan` should say that isn't an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    pub at: Position,
    pub message: String,
}
