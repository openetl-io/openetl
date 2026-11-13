use std::collections::BTreeMap;

use arrow_schema::SchemaRef;
use datafusion_sql::sqlparser::ast;

use crate::{Error, ErrorKind};

/// A stream statements can read: the source's, under its names and logical
/// types.
#[derive(Clone, Debug)]
pub struct Stream {
    pub schema: SchemaRef,
    /// The primary key's columns; empty if it has none.
    pub key: Vec<String>,
    /// Its rows are inserts, updates and deletes of their key, as CDC's are.
    pub changelog: bool,
}

/// The streams statements can read, by their whole names: what `openetl
/// discover` lists, such as `orders`, `public.orders` or `HR.EMPLOYEES`.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    streams: BTreeMap<String, Stream>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a stream. Refuses a name already added, and a key column the
    /// schema doesn't have.
    pub fn insert(&mut self, name: impl Into<String>, stream: Stream) -> Result<(), Error> {
        let name = name.into();
        if let Some(k) = stream
            .key
            .iter()
            .find(|k| stream.schema.field_with_name(k).is_err())
        {
            return Err(Error::new(
                ErrorKind::Catalog,
                None,
                format!("stream {name}'s key column {k} isn't in its schema"),
            ));
        }
        if self.streams.contains_key(&name) {
            return Err(Error::new(
                ErrorKind::Catalog,
                None,
                format!("stream {name} is in the catalog twice"),
            ));
        }
        self.streams.insert(name, stream);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Stream> {
        self.streams.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.streams.keys().map(String::as_str)
    }

    /// The streams whose whole names a written name fits.
    pub(crate) fn fitting(&self, parts: &[ast::Ident]) -> Vec<&str> {
        self.names().filter(|n| fits(n, parts)).collect()
    }
}

/// Whether a name is a written dotted path: its parts joined with dots, each
/// unquoted part compared without regard to case and each quoted one exactly.
/// The parts aren't interpreted, so a part may hold dots of its own.
pub(crate) fn fits(name: &str, parts: &[ast::Ident]) -> bool {
    let mut rest = name.chars();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 && rest.next() != Some('.') {
            return false;
        }
        for want in part.value.chars() {
            let Some(got) = rest.next() else {
                return false;
            };
            let same = match part.quote_style {
                Some(_) => want == got,
                None => want.to_lowercase().eq(got.to_lowercase()),
            };
            if !same {
                return false;
            }
        }
    }
    rest.next().is_none()
}

/// A name as written, for messages.
pub(crate) fn written(parts: &[ast::Ident]) -> String {
    parts
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unquoted(s: &str) -> ast::Ident {
        ast::Ident::new(s)
    }

    fn quoted(s: &str) -> ast::Ident {
        ast::Ident::with_quote('"', s)
    }

    #[test]
    fn a_dotted_path_fits_a_whole_name() {
        assert!(fits(
            "public.orders",
            &[unquoted("public"), unquoted("orders")]
        ));
        assert!(fits(
            "public.orders",
            &[unquoted("PUBLIC"), unquoted("Orders")]
        ));
        assert!(fits("public.orders", &[quoted("public.orders")]));
        assert!(fits(
            "HR.EMPLOYEES",
            &[unquoted("hr"), unquoted("employees")]
        ));
        assert!(fits("a.b.c", &[unquoted("a"), quoted("b.c")]));
        assert!(fits("a.b.c", &[quoted("a.b"), unquoted("C")]));
        assert!(!fits("public.orders", &[unquoted("orders")]));
        assert!(!fits("public.orders", &[quoted("Public.orders")]));
        assert!(!fits(
            "public.orders",
            &[unquoted("public"), unquoted("order")]
        ));
        assert!(!fits("public.orders", &[unquoted("public.orders.x")]));
        assert!(!fits("orders", &[unquoted("public"), unquoted("orders")]));
    }
}
