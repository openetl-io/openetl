//! Destination names for tables and columns. A name, once given, is kept in
//! the stream's schema and restored with it, so it never changes.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow_schema::{Field, Schema};
use twox_hash::XxHash3_64;

/// Schema metadata: a JSON object from each renamed column's destination
/// name to its source name. Field metadata stays as the source sent it,
/// because some Parquet readers mishandle extension types with extra keys.
pub const SOURCE_NAMES_KEY: &str = "openetl.source_names";
/// Schema metadata: the stream's destination table.
pub const TABLE_KEY: &str = "openetl.table";

/// Columns the engine adds. Source columns never get these names.
pub const METADATA_COLUMNS: [&str; 5] = [
    "_openetl_id",
    "_openetl_op",
    "_openetl_seq",
    "_openetl_synced_at",
    "_openetl_deleted",
];
/// Tables and directories sinks keep for themselves.
pub const RESERVED_TABLES: [&str; 3] = ["_openetl", "_openetl_fence", "_openetl_loads"];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Naming {
    /// Keep source names as they are, for destinations that quote them.
    pub keep_source: bool,
    /// The destination's identifier limit in bytes; 0 for none.
    pub max_len: usize,
}

impl Naming {
    /// The name before collisions are resolved. Normalized names are
    /// lowercase, with `_` for anything outside `[a-z0-9_]` and before a
    /// leading digit. Past `max_len`, a name is cut and given a hash of the
    /// source name.
    pub fn base(&self, source: &str) -> String {
        let mut name = if self.keep_source {
            source.to_owned()
        } else {
            let mut out: String = source
                .chars()
                .flat_map(char::to_lowercase)
                .map(|c| {
                    if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            if out.starts_with(|c: char| c.is_ascii_digit()) {
                out.insert(0, '_');
            }
            out
        };
        if name.is_empty() {
            name.push('_');
        }
        if self.max_len > 0 && name.len() > self.max_len {
            let hash = XxHash3_64::oneshot(source.as_bytes()) as u32;
            name = self.fit(&name, &format!("_{hash:08x}"));
        }
        name
    }

    /// `base` with `suffix`, cut at a character boundary to fit `max_len`.
    fn fit(&self, base: &str, suffix: &str) -> String {
        let mut end = base.len();
        if self.max_len > 0 {
            end = end.min(self.max_len.saturating_sub(suffix.len()));
            while !base.is_char_boundary(end) {
                end -= 1;
            }
        }
        format!("{}{suffix}", &base[..end])
    }
}

/// Gives destination names within one scope: a stream's columns, or a job's
/// tables. Names that collide get `_2`, `_3` and so on, in the order they
/// were asked for.
#[derive(Clone, Debug)]
pub struct Namer {
    naming: Naming,
    names: HashMap<String, String>,
    taken: HashSet<String>,
}

impl Namer {
    pub fn new(naming: Naming, reserved: &[&str]) -> Self {
        Self {
            naming,
            names: HashMap::new(),
            taken: reserved.iter().map(|&r| r.to_owned()).collect(),
        }
    }

    /// Keeps a name given before. False if another source holds it.
    pub fn keep(&mut self, source: &str, dest: &str) -> bool {
        match self.names.get(source) {
            Some(d) => return d == dest,
            None if self.taken.contains(dest) => return false,
            None => {}
        }
        self.taken.insert(dest.to_owned());
        self.names.insert(source.to_owned(), dest.to_owned());
        true
    }

    pub fn name(&mut self, source: &str) -> String {
        if let Some(d) = self.names.get(source) {
            return d.clone();
        }
        let base = self.naming.base(source);
        let mut dest = base.clone();
        let mut n = 2;
        while self.taken.contains(&dest) {
            dest = self.naming.fit(&base, &format!("_{n}"));
            n += 1;
        }
        self.keep(source, &dest);
        dest
    }

    /// Renames the fields, and records the source names that changed in the
    /// schema's metadata.
    pub fn rename(&mut self, schema: &Schema) -> Schema {
        let mut renamed = BTreeMap::new();
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| {
                let dest = self.name(f.name());
                if dest != *f.name() {
                    renamed.insert(dest.clone(), f.name().clone());
                }
                f.as_ref().clone().with_name(dest)
            })
            .collect();
        let mut md = schema.metadata().clone();
        md.remove(SOURCE_NAMES_KEY);
        if !renamed.is_empty()
            && let Ok(json) = serde_json::to_string(&renamed)
        {
            md.insert(SOURCE_NAMES_KEY.to_owned(), json);
        }
        Schema::new_with_metadata(fields, md)
    }
}

/// Each renamed column's source name, by destination name. None if the
/// metadata is malformed.
pub fn source_names(schema: &Schema) -> Option<HashMap<String, String>> {
    match schema.metadata().get(SOURCE_NAMES_KEY) {
        Some(json) => serde_json::from_str(json).ok(),
        None => Some(HashMap::new()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn normalized(max_len: usize) -> Naming {
        Naming {
            keep_source: false,
            max_len,
        }
    }

    #[test]
    fn normalizes_names() {
        let n = normalized(0);
        for (source, want) in [
            ("id", "id"),
            ("FooBar", "foobar"),
            ("first name", "first_name"),
            ("e-mail.address", "e_mail_address"),
            ("1st", "_1st"),
            ("Ünïcode", "_n_code"),
            ("", "_"),
            ("_x", "_x"),
        ] {
            assert_eq!(n.base(source), want, "{source:?}");
        }
    }

    #[test]
    fn keeps_source_names_when_asked() {
        let n = Naming {
            keep_source: true,
            max_len: 0,
        };
        assert_eq!(n.base("First Name"), "First Name");
        assert_eq!(n.base(""), "_");
    }

    #[test]
    fn shortens_long_names_with_a_hash() {
        let n = normalized(16);
        let a = n.base("a_very_long_column_name_one");
        let b = n.base("a_very_long_column_name_two");
        assert_eq!(a.len(), 16);
        assert!(a.starts_with("a_very__"), "{a}");
        assert_ne!(a, b);
        assert_eq!(n.base("a_very_long_column_name_one"), a);

        let keep = Naming {
            keep_source: true,
            max_len: 14,
        };
        let cut = keep.base("ééééééééé");
        assert_eq!(cut.len(), 13, "{cut}");
        assert!(cut.starts_with("éé_"), "{cut}");
    }

    #[test]
    fn collisions_get_suffixes_in_order() {
        let mut namer = Namer::new(normalized(0), &METADATA_COLUMNS);
        assert_eq!(namer.name("FooBar"), "foobar");
        assert_eq!(namer.name("foobar"), "foobar_2");
        assert_eq!(namer.name("FOOBAR"), "foobar_3");
        assert_eq!(namer.name("FooBar"), "foobar");
        assert_eq!(namer.name("_openetl_id"), "_openetl_id_2");
    }

    #[test]
    fn suffixes_fit_the_limit() {
        let mut namer = Namer::new(normalized(6), &[]);
        assert_eq!(namer.name("abcdef"), "abcdef");
        assert_eq!(namer.name("ABCDEF"), "abcd_2");
    }

    #[test]
    fn kept_names_win() {
        let mut namer = Namer::new(normalized(0), &[]);
        assert!(namer.keep("foobar", "foobar"));
        assert!(namer.keep("FooBar", "foobar_2"));
        assert!(!namer.keep("other", "foobar"));
        assert!(!namer.keep("FooBar", "x"));
        assert_eq!(namer.name("FooBar"), "foobar_2");
        assert_eq!(namer.name("FOOBAR"), "foobar_3");
    }

    #[test]
    fn renames_fields_and_notes_their_source() {
        let mut namer = Namer::new(normalized(0), &[]);
        let schema = Schema::new(vec![
            Field::new("Id", DataType::Int64, true),
            Field::new("id", DataType::Utf8, true),
        ]);
        let renamed = namer.rename(&schema);
        let names: Vec<_> = renamed.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["id", "id_2"]);
        assert_eq!(
            renamed.metadata().get(SOURCE_NAMES_KEY).unwrap(),
            r#"{"id":"Id","id_2":"id"}"#
        );
        assert!(renamed.field(0).metadata().is_empty());
        assert_eq!(renamed.field(1).data_type(), &DataType::Utf8);
        let names = source_names(&renamed).unwrap();
        assert_eq!(names.get("id_2").map(String::as_str), Some("id"));

        let same = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        assert!(namer.rename(&same).metadata().is_empty());
    }
}
