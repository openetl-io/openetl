//! Control columns: a changelog stream's typed batches carry each record's
//! op and sequence, and the columns an update didn't send, in columns marked
//! by their field metadata. Their names don't matter.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Schema};

use crate::Error;
use crate::ids;

pub const CONTROL_KEY: &str = "openetl.control";
const OP: &str = "op";
const SEQ: &str = "seq";
const UNCHANGED: &str = "unchanged";

const OPS: [&str; 3] = ["insert", "update", "delete"];

/// One batch's control columns, row for row with its records.
#[derive(Debug, Default, Clone)]
pub(crate) struct Control {
    /// Utf8; a null is an insert.
    pub op: Option<ArrayRef>,
    pub seq: Option<ArrayRef>,
    /// A list of Utf8: source column names.
    pub unchanged: Option<ArrayRef>,
}

impl Control {
    /// Takes the control columns out of a batch.
    pub fn split(batch: &RecordBatch) -> Result<(RecordBatch, Control), Error> {
        let schema = batch.schema();
        if !schema
            .fields()
            .iter()
            .any(|f| f.metadata().contains_key(CONTROL_KEY))
        {
            return Ok((batch.clone(), Control::default()));
        }
        let mut c = Control::default();
        let mut fields = Vec::with_capacity(schema.fields().len());
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (f, a) in schema.fields().iter().zip(batch.columns()) {
            let slot = match f.metadata().get(CONTROL_KEY).map(String::as_str) {
                None => {
                    fields.push(f.clone());
                    columns.push(a.clone());
                    continue;
                }
                Some(OP) => &mut c.op,
                Some(SEQ) => &mut c.seq,
                Some(UNCHANGED) => &mut c.unchanged,
                Some(other) => {
                    return Err(Error::Declared(format!(
                        "control column {:?} is {other:?}, which isn't op, seq or unchanged",
                        f.name()
                    )));
                }
            };
            if slot.replace(a.clone()).is_some() {
                return Err(Error::Declared(format!(
                    "two control columns are {:?}",
                    f.metadata()[CONTROL_KEY]
                )));
            }
        }
        if let Some(op) = &c.op {
            if op.data_type() != &DataType::Utf8 {
                return Err(Error::Declared(format!(
                    "the op control column is {}, not utf8",
                    op.data_type()
                )));
            }
            if let Some(bad) = op
                .as_string::<i32>()
                .iter()
                .flatten()
                .find(|v| !OPS.contains(v))
            {
                return Err(Error::Declared(format!(
                    "op {bad:?} isn't insert, update or delete"
                )));
            }
        }
        if let Some(seq) = &c.seq
            && !ids::supported(seq.data_type())
        {
            return Err(Error::Declared(format!(
                "the seq control column is {}, which can't order versions",
                seq.data_type()
            )));
        }
        if let Some(u) = &c.unchanged
            && !matches!(u.data_type(), DataType::List(f) if f.data_type() == &DataType::Utf8)
        {
            return Err(Error::Declared(format!(
                "the unchanged control column is {}, not a list of utf8",
                u.data_type()
            )));
        }
        let opts = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let data = RecordBatch::try_new_with_options(
            std::sync::Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            columns,
            &opts,
        )?;
        Ok((data, c))
    }

    /// Keeps the rows where `keep` is true.
    pub fn filter(&self, keep: &BooleanArray) -> Result<Control, Error> {
        let f = |a: &Option<ArrayRef>| -> Result<Option<ArrayRef>, Error> {
            Ok(match a {
                Some(a) => Some(arrow_select::filter::filter(a, keep)?),
                None => None,
            })
        };
        Ok(Control {
            op: f(&self.op)?,
            seq: f(&self.seq)?,
            unchanged: f(&self.unchanged)?,
        })
    }

    /// Without the rows in `gone`, out of `n`.
    pub fn without(
        &self,
        n: usize,
        gone: &std::collections::HashSet<usize>,
    ) -> Result<Control, Error> {
        if gone.is_empty() || (self.op.is_none() && self.seq.is_none() && self.unchanged.is_none())
        {
            return Ok(self.clone());
        }
        self.filter(&BooleanArray::from_iter(
            (0..n).map(|i| Some(!gone.contains(&i))),
        ))
    }
}
