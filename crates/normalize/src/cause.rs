//! Why a row was quarantined, in fields a machine reads. A cause names a
//! code and a column, never a value.

/// What kind of problem quarantined a row. The set is closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Code {
    /// A raw record isn't JSON.
    Json,
    /// A raw record is JSON, but not an object.
    Object,
    /// A value isn't of its column's type.
    Type,
    /// A value is out of its type's range, or an infinite date or timestamp
    /// that isn't clamped.
    Range,
    /// A decimal has more digits than its column takes.
    Precision,
    /// A NaN that its column's type can't hold.
    Nan,
    /// A SQL transform's statement can't evaluate the row.
    Statement,
    /// A changelog's update didn't send a column the statement needs.
    Unsent,
    /// The row's windows closed past their allowed lateness.
    Late,
    /// The row's event time is null.
    EventTime,
    /// A window's aggregate doesn't fit its type.
    Aggregate,
}

impl Code {
    pub const ALL: [Code; 11] = [
        Code::Json,
        Code::Object,
        Code::Type,
        Code::Range,
        Code::Precision,
        Code::Nan,
        Code::Statement,
        Code::Unsent,
        Code::Late,
        Code::EventTime,
        Code::Aggregate,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Code::Json => "json",
            Code::Object => "object",
            Code::Type => "type",
            Code::Range => "range",
            Code::Precision => "precision",
            Code::Nan => "nan",
            Code::Statement => "statement",
            Code::Unsent => "unsent",
            Code::Late => "late",
            Code::EventTime => "event_time",
            Code::Aggregate => "aggregate",
        }
    }

    /// The code a value's reason starts with: `type`, `range`, `precision`
    /// or `nan`, as coercion and the schema policy give them. A reason
    /// without one of those is `type`.
    pub fn of_reason(reason: &str) -> Code {
        match reason.split_once(':').map(|(c, _)| c) {
            Some("range") => Code::Range,
            Some("precision") => Code::Precision,
            Some("nan") => Code::Nan,
            _ => Code::Type,
        }
    }
}

/// Why a stream that holds cells quarantined a row instead. The set is
/// closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NotHeld {
    /// The column is in the stream's key.
    Key,
    /// The column is the stream's cursor.
    Cursor,
    /// The destination can't hold the column, such as a partition column.
    Destination,
    /// The destination's column takes no null, and it has no heldValue.
    NotNull,
    /// The column's heldValue doesn't fit its type.
    HeldValue,
    /// The record isn't JSON, or isn't an object: it has no value to hold.
    Record,
    /// A SQL transform's statement reads the held cell, which has no value
    /// to evaluate.
    Transform,
}

impl NotHeld {
    pub const ALL: [NotHeld; 7] = [
        NotHeld::Key,
        NotHeld::Cursor,
        NotHeld::Destination,
        NotHeld::NotNull,
        NotHeld::HeldValue,
        NotHeld::Record,
        NotHeld::Transform,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            NotHeld::Key => "key",
            NotHeld::Cursor => "cursor",
            NotHeld::Destination => "destination",
            NotHeld::NotNull => "not_null",
            NotHeld::HeldValue => "held_value",
            NotHeld::Record => "record",
            NotHeld::Transform => "transform",
        }
    }

    /// Why, as a clause about the column.
    pub fn clause(&self) -> &'static str {
        match self {
            NotHeld::Key => "it's part of the stream's key",
            NotHeld::Cursor => "it's the stream's cursor",
            NotHeld::Destination => "the destination can't hold it",
            NotHeld::NotNull => "it can't be null, and the pipeline gives it no held value",
            NotHeld::HeldValue => "its held value doesn't fit its type",
            NotHeld::Record => "its record isn't a JSON object",
            NotHeld::Transform => "a SQL transform's statement reads it",
        }
    }
}

/// Why a row was quarantined. It never holds a value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cause {
    pub code: Code,
    /// The column that caused it, by the name the stream or output gives
    /// it; None where no one column did.
    pub column: Option<String>,
    /// Set where the stream holds cells: why this row's wasn't held.
    pub not_held: Option<NotHeld>,
}

impl Cause {
    pub fn new(code: Code) -> Self {
        Self {
            code,
            column: None,
            not_held: None,
        }
    }

    /// A value of `column` that doesn't fit, from its reason's code.
    pub fn value(reason: &str, column: &str, not_held: Option<NotHeld>) -> Self {
        Self {
            code: Code::of_reason(reason),
            column: Some(column.to_owned()),
            not_held,
        }
    }

    pub fn column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    pub fn not_held(mut self, why: NotHeld) -> Self {
        self.not_held = Some(why);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_distinct_and_snake_case() {
        let codes: std::collections::HashSet<_> = Code::ALL.iter().map(Code::as_str).collect();
        assert_eq!(codes.len(), Code::ALL.len());
        let why: std::collections::HashSet<_> = NotHeld::ALL.iter().map(NotHeld::as_str).collect();
        assert_eq!(why.len(), NotHeld::ALL.len());
        for s in codes.iter().chain(&why) {
            assert!(s.chars().all(|c| c.is_ascii_lowercase() || c == '_'), "{s}");
        }
    }

    #[test]
    fn a_reason_gives_its_code() {
        assert_eq!(
            Code::of_reason("range: column \"a\" is infinite"),
            Code::Range
        );
        assert_eq!(Code::of_reason("precision: x"), Code::Precision);
        assert_eq!(Code::of_reason("nan: x"), Code::Nan);
        assert_eq!(Code::of_reason("type: column \"a:b\" isn't x"), Code::Type);
    }
}
