//! SQL transforms: a transform's statements, and the schemas of the streams
//! they read, planned into a plan of OpenETL's own whose expressions evaluate
//! on Arrow batches.
//!
//! A transform is `INSERT INTO output SELECT …` statements, which may read
//! views that `CREATE VIEW name AS SELECT …` defines before them. `FROM`
//! names a source's stream or a view, never an output; `source.` before a
//! stream's name and `destination.` before an output's are optional. A
//! stream's name is its whole name, matched as a dotted path or quoted.
//! Each statement is classified, and only row-wise statements plan for now:
//! the other classes are refused as a later tier's, with their position.
//!
//! This is the only crate that uses DataFusion: sqlparser parses, and
//! DataFusion binds, coerces types, simplifies and evaluates expressions.
//! None of its types are in this crate's API, and nothing it plans is kept:
//! a plan is made again from the SQL and the streams' schemas.

mod bind;
mod catalog;
mod error;
mod expr;
mod functions;
mod json;
mod lower;
mod plan;
mod sql;
mod types;

pub use catalog::{Catalog, Stream};
pub use error::{Error, ErrorKind, Errors, Note, Position};
pub use expr::Expr;
pub use plan::{
    Class, Column, Operator, Plan, Projection, RowWise, Statement, Step, Unnest, Unnested, View,
};
pub use sql::{Sql, plan};
pub use types::{cast_type, ident};
