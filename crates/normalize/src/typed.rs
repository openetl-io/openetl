//! The first half of normalization: a stream's raw rows parsed, or its typed
//! batches split from their control columns, and given their logical and
//! declared types, under the source's column names. The second half,
//! [`Shaper::shape`](crate::Shaper::shape), names columns for the
//! destination, adds the metadata columns and holds.

use std::collections::{HashMap, HashSet};

use arrow_array::{BooleanArray, FixedSizeBinaryArray, RecordBatch};
use arrow_schema::{Schema, SchemaRef};

use crate::control::Control;
use crate::hold::HeldCell;
use crate::ids::Identity;
use crate::{Error, Normalizer, Rejected, TypeOptions, coerce};

/// A batch between the two halves. A stage between them may change the
/// batch's schema and rows, keeping `ids`, `control`, `cells` and `rows` row
/// for row with it.
#[derive(Debug)]
pub struct Typed {
    /// Under source names, with logical and declared types.
    pub batch: RecordBatch,
    /// Each row's `_openetl_id`.
    pub ids: FixedSizeBinaryArray,
    /// A changelog stream's op, seq and unchanged columns.
    pub control: Control,
    /// Cells held instead of rejecting their rows, by batch row and source
    /// column.
    pub cells: Vec<HeldCell>,
    /// Rows left out, by their index in the input, in order.
    pub rejected: Vec<Rejected>,
    /// Each batch row's index in the input. None: the input's rows less the
    /// rejected, in order.
    pub rows: Option<Vec<usize>>,
}

/// The index in the input of each of a batch's `n` rows, from [`Typed::rows`]
/// and [`Typed::rejected`].
pub(crate) fn input_rows(rows: Option<Vec<usize>>, rejected: &[Rejected], n: usize) -> Vec<usize> {
    if let Some(rows) = rows {
        return rows;
    }
    let gone: HashSet<usize> = rejected.iter().map(|r| r.row).collect();
    (0..).filter(|r| !gone.contains(r)).take(n).collect()
}

/// One lane's first half for a stream. Raw rows' inferred types are its own
/// until it's forked again.
#[derive(Debug)]
pub(crate) struct Typer {
    pub identity: Identity,
    /// Under source names.
    pub declared: Option<SchemaRef>,
    pub types: TypeOptions,
    /// Typed batches must carry op and seq control columns.
    pub changelog: bool,
    /// Some: a declared value that doesn't fit holds its cell instead of
    /// rejecting its row, except in these source columns.
    pub hold_cells: Option<HashSet<String>>,
    norm: Option<Normalizer>,
}

impl Typer {
    pub fn new(identity: Identity, declared: Option<SchemaRef>, types: TypeOptions) -> Self {
        Self {
            identity,
            declared,
            types,
            changelog: false,
            hold_cells: None,
            norm: None,
        }
    }

    /// A copy that hasn't parsed anything.
    pub fn fork(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            declared: self.declared.clone(),
            types: self.types,
            changelog: self.changelog,
            hold_cells: self.hold_cells.clone(),
            norm: None,
        }
    }

    /// Stops adding and widening raw rows' columns.
    pub fn freeze(&mut self) {
        if let Some(n) = self.norm.as_mut() {
            n.freeze();
        }
    }

    /// Parses raw rows. The first call infers from `live`, the stream's
    /// schema under source names, and doesn't add or widen columns if
    /// `frozen`. On error, the inferred schema is unchanged.
    pub fn rows<'a, I>(
        &mut self,
        rows: I,
        live: impl FnOnce() -> Schema,
        frozen: bool,
    ) -> Result<Typed, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let norm = match self.norm.take() {
            Some(n) => n,
            None => self.start(&live(), frozen)?,
        };
        let n = self.norm.insert(norm).push(rows)?;
        let ids = n.ids.ok_or_else(|| Error::Restored(String::new()))?;
        Ok(Typed {
            batch: n.batch,
            ids,
            control: Control::default(),
            cells: n.cells,
            rejected: n.rejected,
            rows: Some(n.rows),
        })
    }

    fn start(&self, live: &Schema, frozen: bool) -> Result<Normalizer, Error> {
        let declared = self.declared.as_deref();
        let mut n = match live.fields().is_empty() {
            true => Normalizer::new(declared)?,
            false => Normalizer::with_schema(live, declared)?,
        };
        n.identify(self.identity.clone());
        n.infinity(self.types.infinity);
        if let Some(unholdable) = &self.hold_cells {
            n.hold_cells(unholdable.clone());
        }
        if frozen {
            n.freeze();
        }
        Ok(n)
    }

    /// Gives a typed batch its logical and declared types, with a changelog
    /// stream's control columns split off.
    pub fn batch(&self, batch: &RecordBatch) -> Result<Typed, Error> {
        let (batch, control) = Control::split(batch)?;
        let batch = &batch;
        if self.changelog && (control.op.is_none() || control.seq.is_none()) {
            return Err(Error::Declared(
                "a changelog stream's batch has no op or seq control column".into(),
            ));
        }
        let (typed, all) = coerce::conform_cells(batch, self.declared.as_deref(), self.types)?;
        let schema = batch.schema();
        let mut first: HashMap<usize, String> = HashMap::new();
        let mut cells = Vec::new();
        for (row, col, reason) in all {
            let name = schema.field(col).name();
            match &self.hold_cells {
                Some(unholdable) if !unholdable.contains(name) => cells.push(HeldCell {
                    row,
                    column: name.clone(),
                    reason,
                    raw: coerce::cell_text(batch.column(col), row).unwrap_or_default(),
                }),
                _ => {
                    first.entry(row).or_insert(reason);
                }
            }
        }
        let mut failed: Vec<(usize, String)> = first.into_iter().collect();
        failed.sort_by_key(|(r, _)| *r);
        let gone: HashSet<usize> = failed.iter().map(|(r, _)| *r).collect();
        let typed = match gone.is_empty() {
            true => typed,
            false => {
                let keep = BooleanArray::from_iter(
                    (0..batch.num_rows()).map(|r| Some(!gone.contains(&r))),
                );
                arrow_select::filter::filter_record_batch(&typed, &keep)?
            }
        };
        let cells = remap(cells, batch.num_rows(), &gone);
        let control = control.without(batch.num_rows(), &gone)?;
        let ids = self.identity.typed(&typed, control.seq.as_ref())?;
        Ok(Typed {
            batch: typed,
            ids,
            control,
            cells,
            rejected: failed
                .into_iter()
                .map(|(row, reason)| Rejected { row, reason })
                .collect(),
            rows: None,
        })
    }
}

/// Cells by their rows among `n`, renumbered once the rows in `gone` are
/// left out, and dropped with them.
pub(crate) fn remap(cells: Vec<HeldCell>, n: usize, gone: &HashSet<usize>) -> Vec<HeldCell> {
    if gone.is_empty() {
        return cells;
    }
    let mut row_of = vec![None; n];
    let mut k = 0;
    for (r, slot) in row_of.iter_mut().enumerate() {
        if !gone.contains(&r) {
            *slot = Some(k);
            k += 1;
        }
    }
    cells
        .into_iter()
        .filter_map(|mut c| {
            c.row = row_of[c.row]?;
            Some(c)
        })
        .collect()
}
