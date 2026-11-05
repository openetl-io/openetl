//! Column holds and cell holds (ADR-0025): what a stream holds back while
//! the rest of each row is delivered, the batches its held values are kept
//! in, and the patches that fill them in later.

use std::collections::{HashMap, HashSet};

use arrow_schema::Field;

/// Lists a row's held columns by destination name. In a patch, the columns
/// the row fills.
pub const HELD_COLUMN: &str = "_openetl_held";
/// Field metadata, "last", on a column whose held cells keep the target's
/// value.
pub const HELD_KEY: &str = "openetl.held";
/// Field metadata on a held batch's columns: "cell" for values kept as text
/// because they didn't fit, "column" for a held column's values.
pub const HOLD_KIND_KEY: &str = "openetl.hold";
pub const CELL: &str = "cell";
pub const COLUMN: &str = "column";
/// Field metadata on a patch's column that fills another held column than
/// its own: col__v2 fills col.
pub const FILLS_KEY: &str = "openetl.fills";

/// What a held cell shows. Values are JSON text of the column's delivered
/// type.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Show {
    #[default]
    Null,
    Value(String),
    /// The target keeps its value; rows it doesn't have get the value, or
    /// null.
    Last(Option<String>),
}

impl Show {
    pub fn value(&self) -> Option<&str> {
        match self {
            Show::Null | Show::Last(None) => None,
            Show::Value(v) | Show::Last(Some(v)) => Some(v),
        }
    }
}

/// Why a column is held whole.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Why {
    /// Not recorded, as by an older record.
    #[default]
    Unknown,
    /// The schema policy, by name, doesn't take the change.
    Policy(String),
    /// The destination refused the change.
    Refused,
    /// The destination can't change the column's type.
    Fixed,
    /// The destination takes no more columns.
    Full,
    /// Held with another column, by its destination name.
    Group(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnHold {
    /// Its destination name.
    pub column: String,
    /// The field as the destination has it; None if it has none.
    pub delivered: Option<Field>,
    pub reason: String,
    pub why: Why,
    /// Its source name, where the engine reports it; empty otherwise.
    pub source: String,
}

/// What a destination's table can't take, by destination name.
#[derive(Clone, Debug, Default)]
pub struct Limits {
    pub not_null: HashSet<String>,
    pub unholdable: HashSet<String>,
    pub fixed: HashSet<String>,
    pub columns: usize,
    /// 0 for no limit.
    pub max_columns: usize,
}

#[derive(Clone, Debug, Default)]
pub struct HoldConfig {
    /// A value that doesn't fit holds its cell instead of rejecting its row.
    pub cells: bool,
    pub show: Show,
    /// By source name.
    pub columns: HashMap<String, Show>,
    /// Columns held together, by source name.
    pub groups: Vec<Vec<String>>,
    /// Column holds in force.
    pub holding: Vec<ColumnHold>,
    /// Columns whose holds were discarded, by destination name: left out
    /// from now on.
    pub discarded: HashSet<String>,
    pub limits: Limits,
}

impl HoldConfig {
    pub fn show(&self, source: &str) -> &Show {
        self.columns.get(source).unwrap_or(&self.show)
    }
}

/// A held cell: its row, its column's source name, why, and its value as
/// received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldCell {
    pub row: usize,
    pub column: String,
    pub reason: String,
    pub raw: String,
}
