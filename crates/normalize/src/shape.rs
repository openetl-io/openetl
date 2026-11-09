use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions,
    StringArray, TimestampMicrosecondArray, new_null_array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde_json::Value;

use crate::coerce::Target;
use crate::control::Control;
use crate::evolve::{BIG_INTS_KEY, Conformed, Evolver, HoldRules, Policy, VERSIONS_KEY};
use crate::hold::{
    CELL, COLUMN, ColumnHold, FILLS_KEY, HELD_COLUMN, HELD_KEY, HOLD_KIND_KEY, HeldCell,
    HoldConfig, Show, Why,
};
use crate::ids::Identity;
use crate::naming::{METADATA_COLUMNS, Namer, Naming, SOURCE_NAMES_KEY, TABLE_KEY, source_names};
use crate::{
    EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, Error, Normalizer, Rejected, TypeOptions,
    UUID_EXTENSION, coerce,
};

#[derive(Debug)]
pub struct Shaped {
    pub batch: RecordBatch,
    /// Rows left out, by their index in the input.
    pub rejected: Vec<Rejected>,
    /// The stream's schema changed with this batch.
    pub schema_changed: bool,
    /// Columns whose type changed incompatibly, and where their values go.
    pub versioned: Vec<String>,
    /// The held values: a batch of held cells, as text, and one of held
    /// columns' values, each with the rows' keys and metadata columns.
    pub held: Vec<RecordBatch>,
    /// Each held cell's column and why.
    pub cells: Vec<(String, String)>,
}

/// A column held whole, by its stream name.
#[derive(Debug, Clone)]
struct Holding {
    dest: String,
    delivered: Option<Field>,
    reason: String,
    why: Why,
    /// Its values are dropped, not kept.
    discarded: bool,
}

#[derive(Clone, Debug, Default)]
struct Holds {
    config: HoldConfig,
    columns: BTreeMap<String, Holding>,
    /// Held columns not seen yet, by destination name.
    pending: HashMap<String, ColumnHold>,
    /// Source columns whose cells can't be held.
    unholdable: HashSet<String>,
    /// Each held value, as a one-row array, by source name.
    values: HashMap<String, ArrayRef>,
    /// The stream has `_openetl_held`.
    started: bool,
    /// Released columns whose values go to a new version: from the held
    /// column's destination name to the version's source name.
    versioned: HashMap<String, String>,
}

/// One stream's schema as the destination sees it: columns under their
/// destination names, then the metadata columns, with the table in the
/// schema's metadata.
#[derive(Debug)]
pub struct Shaper {
    namer: Namer,
    table: String,
    identity: Identity,
    policy: Policy,
    /// Under source names.
    declared: Option<SchemaRef>,
    types: TypeOptions,
    norm: Option<Normalizer>,
    evolver: Evolver,
    current: Option<SchemaRef>,
    /// current matches the evolver's schema.
    fresh: bool,
    /// Adds `_openetl_deleted`.
    soft_deletes: bool,
    /// Typed batches carry op and seq control columns; adds
    /// `_openetl_unchanged`.
    changelog: bool,
    holds: Option<Holds>,
    /// What lanes share has changed since [`Shaper::take_changed`].
    changed: bool,
}

impl Shaper {
    /// `restored` is a schema [`Shaper::schema`] returned before;
    /// `declared` holds logical types under source names.
    pub fn new(
        naming: Naming,
        table: String,
        identity: Identity,
        policy: Policy,
        restored: Option<SchemaRef>,
        declared: Option<SchemaRef>,
        types: TypeOptions,
    ) -> Result<Self, Error> {
        let mut namer = Namer::new(naming, &METADATA_COLUMNS);
        let mut evolver = Evolver::new(policy, None, BTreeMap::new());
        if let Some(r) = &restored {
            let names = source_names(r)
                .ok_or_else(|| Error::Restored(format!("its {SOURCE_NAMES_KEY} metadata")))?;
            let versions = match r.metadata().get(VERSIONS_KEY) {
                Some(v) => serde_json::from_str(v)
                    .map_err(|_| Error::Restored(format!("its {VERSIONS_KEY} metadata")))?,
                None => BTreeMap::new(),
            };
            let mut fields = Vec::with_capacity(r.fields().len());
            for f in r.fields() {
                if METADATA_COLUMNS.contains(&f.name().as_str()) {
                    continue;
                }
                let name = names.get(f.name()).unwrap_or(f.name());
                if !namer.keep(name, f.name()) {
                    return Err(Error::Restored(format!("two columns named {:?}", f.name())));
                }
                fields.push(f.as_ref().clone().with_name(name));
            }
            evolver = Evolver::new(policy, Some(&Schema::new(fields)), versions);
            if let Some(v) = r.metadata().get(BIG_INTS_KEY) {
                evolver.big_ints = serde_json::from_str(v)
                    .map_err(|_| Error::Restored(format!("its {BIG_INTS_KEY} metadata")))?;
            }
        }
        evolver.infinity = types.infinity;
        let declared = match declared {
            Some(d) if types.nanoseconds => Some(coerce::nanoseconds(&d)),
            d => d,
        };
        Ok(Self {
            namer,
            table,
            identity,
            policy,
            declared,
            types,
            norm: None,
            evolver,
            current: restored,
            fresh: false,
            soft_deletes: false,
            changelog: false,
            holds: None,
            changed: false,
        })
    }

    /// A copy for another lane: everything lanes share, which is
    /// all but what it has parsed.
    pub fn fork(&self) -> Self {
        Self {
            namer: self.namer.clone(),
            table: self.table.clone(),
            identity: self.identity.clone(),
            policy: self.policy,
            declared: self.declared.clone(),
            types: self.types,
            norm: None,
            evolver: self.evolver.clone(),
            current: self.current.clone(),
            fresh: self.fresh,
            soft_deletes: self.soft_deletes,
            changelog: self.changelog,
            holds: self.holds.clone(),
            changed: false,
        }
    }

    /// Whether what lanes share has changed since the last call.
    pub fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }

    /// The primary key's columns under destination names.
    pub fn key_columns(&self) -> Vec<String> {
        self.identity
            .key
            .iter()
            .map(|k| self.namer.get(k).unwrap_or(k).to_owned())
            .collect()
    }

    /// Holds columns and cells back instead of stopping or rejecting rows.
    /// The stream must be keyed.
    pub fn with_holds(mut self, config: HoldConfig) -> Result<Self, Error> {
        if self.identity.key.is_empty() {
            return Err(Error::Declared("only a keyed stream holds columns".into()));
        }
        let restored = self.current.clone();
        let started = restored
            .as_ref()
            .is_some_and(|s| s.field_with_name(HELD_COLUMN).is_ok());
        let source = |namer: &Namer, dest: &str| namer.source_of(dest).map(str::to_owned);
        let mut h = Holds {
            started,
            ..Default::default()
        };
        h.unholdable.extend(self.identity.key.iter().cloned());
        h.unholdable.extend(self.identity.cursor.iter().cloned());
        for d in &config.limits.unholdable {
            h.unholdable.extend(source(&self.namer, d));
        }
        let fields: Vec<Field> = self
            .declared
            .iter()
            .flat_map(|d| d.fields().iter().map(|f| f.as_ref().clone()))
            .chain(
                self.evolver
                    .live()
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone()),
            )
            .collect();
        for f in &fields {
            let show = config.show(f.name());
            if let Some(v) = show.value() {
                match held_value(f, v) {
                    Some(a) => {
                        h.values.insert(f.name().clone(), a);
                    }
                    None => {
                        h.unholdable.insert(f.name().clone());
                    }
                }
            }
        }
        for d in &config.limits.not_null {
            if let Some(src) = source(&self.namer, d)
                && config.show(&src).value().is_none()
            {
                h.unholdable.insert(src);
            }
        }
        let mut rules = HoldRules {
            unholdable: h.unholdable.clone(),
            groups: config.groups.clone(),
            cells: config.cells,
            columns_left: (config.limits.max_columns > 0).then(|| {
                config
                    .limits
                    .max_columns
                    .saturating_sub(config.limits.columns)
            }),
            ..Default::default()
        };
        for d in &config.limits.fixed {
            rules.fixed.extend(source(&self.namer, d));
        }
        for c in &config.holding {
            let discarded = config.discarded.contains(&c.column);
            match source(&self.namer, &c.column) {
                Some(src) => {
                    self.evolver.holding.insert(src.clone());
                    h.columns.insert(
                        src,
                        Holding {
                            dest: c.column.clone(),
                            delivered: c.delivered.clone(),
                            reason: c.reason.clone(),
                            why: c.why.clone(),
                            discarded,
                        },
                    );
                }
                None => {
                    h.pending.insert(c.column.clone(), c.clone());
                }
            }
        }
        if config.cells {
            rules.cells = true;
        }
        self.evolver.rules = Some(rules);
        h.config = config;
        self.holds = Some(h);
        self.fresh = false;
        Ok(self)
    }

    /// The schema the destination gets, where columns are held whole; None
    /// where it's the stream's.
    pub fn delivered(&self) -> Option<SchemaRef> {
        let h = self.holds.as_ref()?;
        let current = self.current.as_ref()?;
        if h.columns.is_empty() {
            return None;
        }
        Some(Arc::new(deliver_schema(current, &h.columns)))
    }

    /// The columns held whole, discarded ones included.
    pub fn holding(&self) -> Vec<ColumnHold> {
        let Some(h) = &self.holds else {
            return Vec::new();
        };
        h.columns
            .iter()
            .map(|(src, c)| ColumnHold {
                column: c.dest.clone(),
                delivered: c.delivered.clone(),
                reason: c.reason.clone(),
                why: c.why.clone(),
                source: src.clone(),
            })
            .collect()
    }

    /// Before a batch reaches the evolver: held columns it hasn't seen
    /// yet, named by their destination names, become held.
    fn claim_pending(&mut self, schema: &Schema) {
        let Some(h) = &mut self.holds else { return };
        if h.pending.is_empty() {
            return;
        }
        for f in schema.fields() {
            let dest = self.namer.name(f.name());
            if let Some(c) = h.pending.remove(&dest) {
                self.changed = true;
                self.evolver.holding.insert(f.name().clone());
                h.columns.insert(
                    f.name().clone(),
                    Holding {
                        dest,
                        delivered: c.delivered,
                        reason: c.reason,
                        why: c.why,
                        discarded: h.config.discarded.contains(&c.column),
                    },
                );
            }
        }
    }

    /// Adds `_openetl_deleted`, true where `_openetl_op` is `delete`, for
    /// merge writes that keep deleted rows.
    pub fn with_soft_deletes(mut self, on: bool) -> Self {
        self.soft_deletes = on;
        self
    }

    /// For a changelog stream (CDC).
    pub fn with_changelog(mut self, on: bool) -> Self {
        self.changelog = on;
        self
    }

    /// None until the stream has a column.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.current.clone()
    }

    /// Takes the declared types of the stream's columns into its schema
    /// before any row arrives, as a batch of them would, so a replay's
    /// patches have them. True if the schema changed.
    pub fn declare(&mut self) -> Result<bool, Error> {
        let (Some(declared), Some(current)) = (self.declared.clone(), self.current.clone()) else {
            return Ok(false);
        };
        let live = self.evolver.live();
        let known: Vec<Field> = declared
            .fields()
            .iter()
            .filter(|f| live.field_with_name(f.name()).is_ok())
            .map(|f| f.as_ref().clone())
            .collect();
        if known.is_empty() {
            return Ok(false);
        }
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(known)));
        let (typed, _) = coerce::conform_cells(&empty, Some(&declared), self.types)?;
        let c = self.evolver.conform(&typed)?;
        if !c.changed {
            return Ok(false);
        }
        let seq = current
            .field_with_name("_openetl_seq")
            .ok()
            .map(|f| f.data_type().clone());
        self.shape(&c.batch, seq.as_ref());
        Ok(true)
    }

    /// Sends each released column hold's values to a new version of its
    /// column, as [`Evolver::version`] does, so a replay's patches fill
    /// that version. True if the schema changed.
    pub fn version(&mut self, columns: &[ColumnHold]) -> Result<bool, Error> {
        let Some(current) = self.current.clone() else {
            return Ok(false);
        };
        let mut changed = false;
        for c in columns {
            let (Some(kept), Some(src)) = (&c.delivered, self.namer.source_of(&c.column)) else {
                continue;
            };
            let src = src.to_owned();
            let before = self.evolver.versions().clone();
            let Some(next) = self.evolver.version(&src, kept) else {
                continue;
            };
            changed |= *self.evolver.versions() != before;
            if let Some(h) = self.holds.as_mut() {
                h.columns.remove(&src);
                h.versioned.insert(c.column.clone(), next);
            }
        }
        if changed {
            let seq = current
                .field_with_name("_openetl_seq")
                .ok()
                .map(|f| f.data_type().clone());
            let empty = RecordBatch::new_empty(Arc::new(self.evolver.schema()));
            self.shape(&empty, seq.as_ref());
        }
        Ok(changed)
    }

    /// Called at each barrier: from now on, the schema policy applies. True
    /// if it didn't before.
    pub fn seal(&mut self) -> bool {
        let before = self.evolver.established();
        self.evolver.establish();
        if self.policy == Policy::IgnoreNew
            && self.evolver.established()
            && let Some(n) = self.norm.as_mut()
        {
            n.freeze();
        }
        self.evolver.established() != before
    }

    /// Converts raw rows. On error, the schema is unchanged.
    pub fn rows<'a, I>(&mut self, rows: I) -> Result<Shaped, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if self.norm.is_none() {
            let declared = self.declared.as_deref();
            let live = self.evolver.live();
            let mut n = match live.fields().is_empty() {
                true => Normalizer::new(declared)?,
                false => Normalizer::with_schema(&live, declared)?,
            };
            n.identify(self.identity.clone());
            n.infinity(self.types.infinity);
            if let Some(h) = self.holds.as_ref().filter(|h| h.config.cells) {
                n.hold_cells(h.unholdable.clone());
            }
            if self.policy == Policy::IgnoreNew && self.evolver.established() {
                n.freeze();
            }
            self.norm = Some(n);
        }
        let Some(norm) = self.norm.as_mut() else {
            return Err(Error::Restored(String::new()));
        };
        let n = norm.push(rows)?;
        let ids = n.ids.ok_or_else(|| Error::Restored(String::new()))?;
        let mut rejected = n.rejected;
        self.claim_pending(&n.batch.schema());
        let c = self.evolver.conform(&n.batch)?;
        rejected.extend(c.failed.iter().map(|(i, reason)| Rejected {
            row: n.rows[*i],
            reason: reason.clone(),
        }));
        rejected.sort_by_key(|r| r.row);
        self.finish(c, ids, Control::default(), rejected, n.cells)
    }

    /// Gives a typed batch its logical and declared types, conforms it to the
    /// stream's schema, renames its columns and adds the metadata columns,
    /// with a changelog stream's op, seq and unchanged columns from its
    /// control columns.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<Shaped, Error> {
        let (batch, control) = Control::split(batch)?;
        let batch = &batch;
        if self.changelog && (control.op.is_none() || control.seq.is_none()) {
            return Err(Error::Declared(
                "a changelog stream's batch has no op or seq control column".into(),
            ));
        }
        let (typed, all) = coerce::conform_cells(batch, self.declared.as_deref(), self.types)?;
        let schema = batch.schema();
        let hold = self.holds.as_ref().filter(|h| h.config.cells);
        let mut first: HashMap<usize, String> = HashMap::new();
        let mut cells = Vec::new();
        for (row, col, reason) in all {
            let name = schema.field(col).name();
            match hold {
                Some(h) if !h.unholdable.contains(name) => cells.push(HeldCell {
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
        self.claim_pending(&typed.schema());
        let c = self.evolver.conform(&typed)?;
        let mut rejected: Vec<Rejected> = failed
            .iter()
            .map(|(row, reason)| Rejected {
                row: *row,
                reason: reason.clone(),
            })
            .collect();
        if !c.failed.is_empty() {
            let gone: HashSet<usize> = failed.iter().map(|(r, _)| *r).collect();
            let kept: Vec<usize> = (0..batch.num_rows())
                .filter(|r| !gone.contains(r))
                .collect();
            rejected.extend(c.failed.iter().map(|(i, reason)| Rejected {
                row: kept[*i],
                reason: reason.clone(),
            }));
            rejected.sort_by_key(|r| r.row);
        }
        self.finish(c, ids, control, rejected, cells)
    }

    fn finish(
        &mut self,
        c: Conformed,
        ids: FixedSizeBinaryArray,
        control: Control,
        rejected: Vec<Rejected>,
        cells: Vec<HeldCell>,
    ) -> Result<Shaped, Error> {
        let gone: HashSet<usize> = c.failed.iter().map(|(r, _)| *r).collect();
        let mut cells = remap(cells, ids.len(), &gone);
        cells.extend(c.cells.iter().cloned());
        let (ids, control) = if c.failed.is_empty() {
            (ids, control)
        } else {
            let keep = BooleanArray::from_iter((0..ids.len()).map(|i| Some(!gone.contains(&i))));
            let kept = arrow_select::filter::filter(&ids, &keep)?;
            (kept.as_fixed_size_binary().clone(), control.filter(&keep)?)
        };
        let started = self.start_holds(&c.held, &cells);
        let seq = control.seq.as_ref().map(|a| a.data_type());
        let schema = match &self.current {
            Some(s)
                if self.fresh
                    && !c.changed
                    && !c.big_ints_grew
                    && !started
                    && seq.is_none_or(|t| {
                        s.field_with_name("_openetl_seq")
                            .is_ok_and(|f| f.data_type() == t)
                    }) =>
            {
                s.clone()
            }
            _ => self.shape(&c.batch, seq),
        };
        let unchanged = self.unchanged(&c.batch, &control)?;
        let lists = self.held_lists(c.batch.num_rows(), unchanged.as_ref(), &cells);
        let batch = self.assemble(schema, &c.batch, ids, &control, unchanged, lists)?;
        let (batch, held) = self.hold_back(batch, &cells)?;
        let cells = cells
            .iter()
            .map(|h| {
                (
                    self.namer.get(&h.column).unwrap_or(&h.column).to_owned(),
                    h.reason.clone(),
                )
            })
            .collect();
        Ok(Shaped {
            batch,
            rejected,
            schema_changed: c.changed || started,
            versioned: c.versioned,
            held,
            cells,
        })
    }

    /// Records the column holds the evolver started, at the type the
    /// destination had. True if the stream gets `_openetl_held` now.
    fn start_holds(&mut self, started: &[(String, String, Why)], cells: &[HeldCell]) -> bool {
        let Some(mut h) = self.holds.take() else {
            return false;
        };
        if !started.is_empty() {
            self.changed = true;
            let prior: HashMap<String, Field> = self
                .delivered()
                .or_else(|| self.current.clone())
                .map(|s| {
                    s.fields()
                        .iter()
                        .map(|f| (f.name().clone(), f.as_ref().clone()))
                        .collect()
                })
                .unwrap_or_default();
            for (src, reason, why) in started {
                let dest = self.namer.name(src);
                let why = match why {
                    Why::Group(with) => Why::Group(self.namer.name(with)),
                    why => why.clone(),
                };
                h.columns.insert(
                    src.clone(),
                    Holding {
                        delivered: prior.get(&dest).cloned(),
                        dest,
                        reason: reason.clone(),
                        why,
                        discarded: false,
                    },
                );
            }
        }
        let now = !h.started && (!cells.is_empty() || h.columns.values().any(|c| !c.discarded));
        h.started |= now;
        self.changed |= now;
        self.holds = Some(h);
        now
    }

    /// Each row's held columns, null where none; None until the stream
    /// holds anything. A column held whole isn't held in a row whose update
    /// didn't send it.
    fn held_lists(
        &self,
        n: usize,
        unchanged: Option<&ArrayRef>,
        cells: &[HeldCell],
    ) -> Option<ArrayRef> {
        let h = self.holds.as_ref().filter(|h| h.started)?;
        let whole: Vec<&str> = h
            .columns
            .values()
            .filter(|c| !c.discarded)
            .map(|c| c.dest.as_str())
            .collect();
        let mut by_row: Vec<Vec<String>> = vec![Vec::new(); n];
        if !whole.is_empty() {
            let unchanged = unchanged.map(string_lists).unwrap_or_else(|| vec![None; n]);
            for (list, skip) in by_row.iter_mut().zip(unchanged) {
                let skip = skip.unwrap_or_default();
                list.extend(
                    whole
                        .iter()
                        .filter(|d| !skip.iter().any(|s| s == **d))
                        .map(|d| (*d).to_owned()),
                );
            }
        }
        for c in cells {
            let dest = self.namer.get(&c.column).unwrap_or(&c.column);
            by_row[c.row].push(dest.to_owned());
        }
        let mut b = ListBuilder::new(StringBuilder::new());
        for list in by_row {
            if list.is_empty() {
                b.append_null();
                continue;
            }
            for d in list {
                b.values().append_value(d);
            }
            b.append(true);
        }
        Some(Arc::new(b.finish()))
    }

    /// Splits the held values off a shaped batch: held columns go to the
    /// type the destination has, or out, and held cells show what the
    /// pipeline chose.
    fn hold_back(
        &self,
        batch: RecordBatch,
        cells: &[HeldCell],
    ) -> Result<(RecordBatch, Vec<RecordBatch>), Error> {
        let Some(h) = self.holds.as_ref().filter(|h| h.started) else {
            return Ok((batch, Vec::new()));
        };
        let n = batch.num_rows();
        let schema = batch.schema();
        let lists = batch.column(schema.index_of(HELD_COLUMN)?).clone();
        let mut held = Vec::new();
        let whole: Vec<(&String, &Holding)> =
            h.columns.iter().filter(|(_, c)| !c.discarded).collect();
        if !whole.is_empty() && n > 0 {
            let cols = whole.iter().map(|(_, c)| {
                let a = batch.column(schema.index_of(&c.dest)?).clone();
                let f = schema.field_with_name(&c.dest)?.clone();
                Ok((f.with_metadata(kind(COLUMN)), a))
            });
            let names: HashSet<&str> = whole.iter().map(|(_, c)| c.dest.as_str()).collect();
            if let Some(b) =
                self.held_batch(&batch, &lists, &names, cols.collect::<Result<_, Error>>()?)?
            {
                held.push(b);
            }
        }
        if !cells.is_empty() {
            let mut by_col: BTreeMap<String, Vec<Option<String>>> = BTreeMap::new();
            for c in cells {
                let dest = self.namer.get(&c.column).unwrap_or(&c.column).to_owned();
                by_col.entry(dest).or_insert_with(|| vec![None; n])[c.row] = Some(c.raw.clone());
            }
            let names: HashSet<&str> = by_col.keys().map(String::as_str).collect();
            let cols = by_col
                .iter()
                .map(|(d, v)| {
                    let f = Field::new(d, DataType::Utf8, true).with_metadata(kind(CELL));
                    (f, Arc::new(StringArray::from(v.clone())) as ArrayRef)
                })
                .collect();
            if let Some(b) = self.held_batch(&batch, &lists, &names, cols)? {
                held.push(b);
            }
        }

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        let mut shown: BTreeMap<usize, Vec<bool>> = BTreeMap::new();
        for c in cells {
            let dest = self.namer.get(&c.column).unwrap_or(&c.column);
            if let Ok(i) = schema.index_of(dest) {
                shown.entry(i).or_insert_with(|| vec![false; n])[c.row] = true;
            }
        }
        for (i, mask) in shown {
            let src = self
                .namer
                .source_of(schema.field(i).name())
                .unwrap_or(schema.field(i).name());
            if let Some(v) = h
                .values
                .get(src)
                .filter(|v| v.data_type() == columns[i].data_type())
            {
                let mask = BooleanArray::from(mask);
                columns[i] = arrow_select::zip::zip(&mask, &broadcast(v, n)?, &columns[i])?;
            }
        }
        let delivered = Arc::new(deliver_schema(&schema, &h.columns));
        let by_dest: HashMap<&str, (&String, &Holding)> = h
            .columns
            .iter()
            .map(|(s, c)| (c.dest.as_str(), (s, c)))
            .collect();
        let mut out = Vec::with_capacity(delivered.fields().len());
        for f in delivered.fields() {
            let a = &columns[schema.index_of(f.name())?];
            out.push(match by_dest.get(f.name().as_str()) {
                None => a.clone(),
                Some((src, _)) => match h
                    .values
                    .get(src.as_str())
                    .filter(|v| v.data_type() == f.data_type())
                {
                    Some(v) => broadcast(v, n)?,
                    None => new_null_array(f.data_type(), n),
                },
            });
        }
        let opts = RecordBatchOptions::new().with_row_count(Some(n));
        Ok((
            RecordBatch::try_new_with_options(delivered, out, &opts)?,
            held,
        ))
    }

    /// The rows of `batch` that hold any of `names`, with their keys, their
    /// metadata columns, their held columns among `names`, and `cols`.
    fn held_batch(
        &self,
        batch: &RecordBatch,
        lists: &ArrayRef,
        names: &HashSet<&str>,
        cols: Vec<(Field, ArrayRef)>,
    ) -> Result<Option<RecordBatch>, Error> {
        let n = batch.num_rows();
        let mut keep = Vec::with_capacity(n);
        let mut b = ListBuilder::new(StringBuilder::new());
        for list in string_lists(lists) {
            let mut any = false;
            for d in list.iter().flatten() {
                if names.contains(d.as_str()) {
                    b.values().append_value(d);
                    any = true;
                }
            }
            b.append(any);
            keep.push(any);
        }
        if !keep.iter().any(|k| *k) {
            return Ok(None);
        }
        let schema = batch.schema();
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut take = |name: &str| -> Result<(), Error> {
            if let Ok(i) = schema.index_of(name) {
                fields.push(schema.field(i).clone());
                arrays.push(batch.column(i).clone());
            }
            Ok(())
        };
        for k in &self.identity.key {
            take(self.namer.get(k).unwrap_or(k))?;
        }
        for m in ["_openetl_id", "_openetl_seq", "_openetl_op"] {
            take(m)?;
        }
        fields.push(Field::new(HELD_COLUMN, held_type(), true));
        arrays.push(Arc::new(b.finish()));
        for (f, a) in cols {
            fields.push(f);
            arrays.push(a);
        }
        let opts = RecordBatchOptions::new().with_row_count(Some(n));
        let all = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &opts)?;
        let keep = BooleanArray::from(keep);
        Ok(Some(arrow_select::filter::filter_record_batch(
            &all, &keep,
        )?))
    }

    /// Patches from a batch of held values, for the released columns: each
    /// row fills those of its held cells that fit their column's type now.
    /// Returns the patches, if any, and how many of the released columns'
    /// cells still don't fit, which stay held. A column held whole again
    /// gets none, since the destination doesn't have its type.
    pub fn patch(
        &self,
        held: &RecordBatch,
        released: &HashSet<String>,
    ) -> Result<(Option<RecordBatch>, u64), Error> {
        let Some(current) = &self.current else {
            return Ok((None, 0));
        };
        let n = held.num_rows();
        let schema = held.schema();
        let lists = string_lists(held.column(schema.index_of(HELD_COLUMN)?));
        let listed = |row: usize, name: &str| {
            lists[row]
                .as_ref()
                .is_some_and(|l| l.iter().any(|d| d == name))
        };
        let mut still = 0u64;
        let mut filled: Vec<(Field, ArrayRef, Vec<bool>)> = Vec::new();
        for (f, col) in schema.fields().iter().zip(held.columns()) {
            let Some(k) = f.metadata().get(HOLD_KIND_KEY) else {
                continue;
            };
            if !released.contains(f.name()) {
                continue;
            }
            // A column whose values go to a new version fills it instead.
            let version = self
                .holds
                .as_ref()
                .and_then(|h| h.versioned.get(f.name()))
                .and_then(|v| self.namer.get(v));
            let into = version.unwrap_or(f.name());
            if self.holds.as_ref().is_some_and(|h| {
                h.columns
                    .values()
                    .any(|c| c.dest == *f.name() || c.dest == into)
            }) {
                if k == CELL {
                    still += (0..n).filter(|r| listed(*r, f.name())).count() as u64;
                }
                continue;
            }
            let source = self.namer.source_of(f.name()).unwrap_or(f.name());
            let declared = self
                .declared
                .as_ref()
                .filter(|_| version.is_none())
                .and_then(|d| d.field_with_name(source).ok());
            let Some(target) = declared.or_else(|| current.field_with_name(into).ok()) else {
                continue;
            };
            let target = target
                .clone()
                .with_name(into)
                .with_nullable(true)
                .with_metadata(HashMap::new());
            let (arr, failed): (ArrayRef, HashSet<usize>) = if k == CELL {
                let Some(t) = Target::of(&target).filter(|t| *t != Target::TypedOnly) else {
                    still += (0..n).filter(|r| listed(*r, f.name())).count() as u64;
                    continue;
                };
                match coerce::text_column(&target, t, col, Default::default()) {
                    Some((a, rows)) => (a, rows.into_iter().map(|(r, _)| r).collect()),
                    None => (new_null_array(target.data_type(), n), (0..n).collect()),
                }
            } else if col.data_type() == target.data_type() {
                (col.clone(), HashSet::new())
            } else {
                let a = match crate::widens(f, &target) {
                    true => crate::evolve::cast_up(col, &target)?,
                    false => arrow_cast::cast(col, target.data_type())
                        .unwrap_or_else(|_| new_null_array(target.data_type(), n)),
                };
                let failed = (0..n)
                    .filter(|&r| !col.is_null(r) && a.is_null(r))
                    .collect();
                (a, failed)
            };
            let mask: Vec<bool> = (0..n)
                .map(|r| listed(r, f.name()) && !failed.contains(&r))
                .collect();
            still += (0..n)
                .filter(|&r| listed(r, f.name()) && failed.contains(&r))
                .count() as u64;
            let target = match version {
                Some(_) => {
                    target.with_metadata(HashMap::from([(FILLS_KEY.to_owned(), f.name().clone())]))
                }
                None => target,
            };
            filled.push((target, arr, mask));
        }
        let keep: Vec<bool> = (0..n)
            .map(|r| filled.iter().any(|(_, _, m)| m[r]))
            .collect();
        if !keep.iter().any(|k| *k) {
            return Ok((None, still));
        }
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for k in &self.identity.key {
            let name = self.namer.get(k).unwrap_or(k);
            let i = schema.index_of(name)?;
            fields.push(schema.field(i).clone());
            arrays.push(held.column(i).clone());
        }
        for m in ["_openetl_id", "_openetl_seq"] {
            if let Ok(i) = schema.index_of(m) {
                fields.push(schema.field(i).clone());
                arrays.push(held.column(i).clone());
            }
        }
        fields.push(Field::new(
            "_openetl_synced_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));
        arrays.push(Arc::new(
            TimestampMicrosecondArray::from_value(now_micros(), n).with_timezone("UTC"),
        ));
        let mut b = ListBuilder::new(StringBuilder::new());
        for (row, k) in keep.iter().enumerate() {
            for (f, _, m) in &filled {
                if m[row] {
                    b.values()
                        .append_value(f.metadata().get(FILLS_KEY).unwrap_or(f.name()));
                }
            }
            b.append(*k);
        }
        fields.push(Field::new(HELD_COLUMN, held_type(), true));
        arrays.push(Arc::new(b.finish()));
        for (f, a, m) in filled {
            let nulls = new_null_array(f.data_type(), n);
            arrays.push(arrow_select::zip::zip(&BooleanArray::from(m), &a, &nulls)?);
            fields.push(f.with_nullable(true));
        }
        let opts = RecordBatchOptions::new().with_row_count(Some(n));
        let schema = Schema::new_with_metadata(fields, current.metadata().clone());
        let all = RecordBatch::try_new_with_options(Arc::new(schema), arrays, &opts)?;
        let out = arrow_select::filter::filter_record_batch(&all, &BooleanArray::from(keep))?;
        Ok((Some(out), still))
    }

    /// The destination schema for a batch under source names. `seq` is the
    /// type of its seq control column.
    fn shape(&mut self, batch: &RecordBatch, seq: Option<&DataType>) -> SchemaRef {
        let source = batch.schema();
        let renamed = self.namer.rename(&source);
        let last = |src: &str| {
            self.holds
                .as_ref()
                .is_some_and(|h| matches!(h.config.show(src), Show::Last(_)))
        };
        let mut fields: Vec<Field> = renamed
            .fields()
            .iter()
            .zip(source.fields())
            .map(|(f, sf)| match last(sf.name()) {
                true => {
                    let mut md = f.metadata().clone();
                    md.insert(HELD_KEY.to_owned(), "last".to_owned());
                    f.as_ref().clone().with_metadata(md)
                }
                false => f.as_ref().clone(),
            })
            .collect();
        let data = !fields.is_empty();
        fields.push(
            Field::new("_openetl_id", DataType::FixedSizeBinary(16), false).with_metadata(
                HashMap::from([
                    (EXTENSION_NAME_KEY.to_owned(), UUID_EXTENSION.to_owned()),
                    (EXTENSION_METADATA_KEY.to_owned(), String::new()),
                ]),
            ),
        );
        fields.push(Field::new("_openetl_op", DataType::Utf8, false));
        match seq.map(|t| Field::new("_openetl_seq", t.clone(), true)) {
            Some(f) => fields.push(f),
            None => match self.seq_field(&source) {
                Some(f) => fields.push(f.with_name("_openetl_seq")),
                None if self.identity.cursor.is_none() => fields.push(Field::new(
                    "_openetl_seq",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                )),
                None => {}
            },
        }
        fields.push(Field::new(
            "_openetl_synced_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));
        if self.soft_deletes {
            fields.push(Field::new("_openetl_deleted", DataType::Boolean, false));
        }
        if self.changelog {
            fields.push(Field::new(
                "_openetl_unchanged",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ));
        }
        if self.holds.as_ref().is_some_and(|h| h.started) {
            fields.push(Field::new(HELD_COLUMN, held_type(), true));
        }
        let mut md = renamed.metadata().clone();
        md.insert(TABLE_KEY.to_owned(), self.table.clone());
        if !self.evolver.versions().is_empty()
            && let Ok(v) = serde_json::to_string(self.evolver.versions())
        {
            md.insert(VERSIONS_KEY.to_owned(), v);
        }
        if !self.evolver.big_ints.is_empty()
            && let Ok(v) = serde_json::to_string(&self.evolver.big_ints)
        {
            md.insert(BIG_INTS_KEY.to_owned(), v);
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, md));
        if data {
            self.changed |= !same_but_big_ints(self.current.as_deref(), &schema);
            self.current = Some(schema.clone());
            self.fresh = true;
        }
        schema
    }

    /// The cursor column's field, once the stream has one.
    fn seq_field(&self, source: &Schema) -> Option<Field> {
        let c = self.identity.cursor.as_ref()?;
        source
            .field_with_name(c)
            .ok()
            .map(|f| f.clone().with_nullable(true))
    }

    /// The unchanged control column under destination names, leaving out
    /// columns the batch doesn't have; None unless the stream is a changelog.
    fn unchanged(
        &mut self,
        batch: &RecordBatch,
        control: &Control,
    ) -> Result<Option<ArrayRef>, Error> {
        if !self.changelog {
            return Ok(None);
        }
        let n = batch.num_rows();
        let mut b = ListBuilder::new(StringBuilder::new());
        let Some(u) = &control.unchanged else {
            for _ in 0..n {
                b.append_null();
            }
            return Ok(Some(Arc::new(b.finish())));
        };
        let schema = batch.schema();
        for names in u.as_list::<i32>().iter() {
            let Some(names) = names else {
                b.append_null();
                continue;
            };
            for name in names.as_string::<i32>().iter().flatten() {
                if schema.field_with_name(name).is_ok() {
                    b.values().append_value(self.namer.name(name));
                }
            }
            b.append(true);
        }
        Ok(Some(Arc::new(b.finish())))
    }

    fn assemble(
        &self,
        schema: SchemaRef,
        batch: &RecordBatch,
        ids: FixedSizeBinaryArray,
        control: &Control,
        unchanged: Option<ArrayRef>,
        held: Option<ArrayRef>,
    ) -> Result<RecordBatch, Error> {
        let n = batch.num_rows();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(ids));
        let ops = match &control.op {
            Some(op) => StringArray::from_iter_values(
                op.as_string::<i32>().iter().map(|v| v.unwrap_or("insert")),
            ),
            None => StringArray::from_iter_values(std::iter::repeat_n("insert", n)),
        };
        let deleted = self
            .soft_deletes
            .then(|| BooleanArray::from_iter(ops.iter().map(|op| Some(op == Some("delete")))));
        columns.push(Arc::new(ops));
        if schema.index_of("_openetl_seq").is_ok() {
            columns.push(match (&control.seq, &self.identity.cursor) {
                (Some(seq), _) => seq.clone(),
                (None, Some(c)) => batch.column(batch.schema().index_of(c)?).clone(),
                (None, None) => Arc::new(
                    TimestampMicrosecondArray::from_value(self.identity.read_start, n)
                        .with_timezone("UTC"),
                ),
            });
        }
        columns.push(Arc::new(
            TimestampMicrosecondArray::from_value(now_micros(), n).with_timezone("UTC"),
        ));
        if let Some(d) = deleted {
            columns.push(Arc::new(d));
        }
        if let Some(u) = unchanged {
            columns.push(u);
        }
        if schema.index_of(HELD_COLUMN).is_ok() {
            columns.push(held.unwrap_or_else(|| new_null_array(&held_type(), n)));
        }
        let opts = RecordBatchOptions::new().with_row_count(Some(n));
        Ok(RecordBatch::try_new_with_options(schema, columns, &opts)?)
    }
}

/// Whether two schemas differ at most in which columns have held big
/// integers, which matters only when restoring.
fn same_but_big_ints(a: Option<&Schema>, b: &Schema) -> bool {
    let Some(a) = a else { return false };
    let without = |s: &Schema| {
        let mut md = s.metadata().clone();
        md.remove(BIG_INTS_KEY);
        md
    };
    a.fields() == b.fields() && without(a) == without(b)
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// A list-of-strings column's rows.
fn string_lists(a: &ArrayRef) -> Vec<Option<Vec<String>>> {
    a.as_list::<i32>()
        .iter()
        .map(|l| {
            l.map(|v| {
                v.as_string::<i32>()
                    .iter()
                    .flatten()
                    .map(str::to_owned)
                    .collect()
            })
        })
        .collect()
}

fn held_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
}

fn kind(k: &str) -> HashMap<String, String> {
    HashMap::from([(HOLD_KIND_KEY.to_owned(), k.to_owned())])
}

/// Cells by their rows among `n`, renumbered once the rows in `gone` are
/// left out, and dropped with them.
fn remap(cells: Vec<HeldCell>, n: usize, gone: &HashSet<usize>) -> Vec<HeldCell> {
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

/// A held value, JSON text, as a one-row array of the field's type; None if
/// it isn't one.
fn held_value(f: &Field, text: &str) -> Option<ArrayRef> {
    let v: Value = serde_json::from_str(text).ok()?;
    let t = Target::of(f).filter(|t| *t != Target::TypedOnly)?;
    let (a, failed) = coerce::json(f, t, std::iter::once(Some(&v)), Default::default()).ok()?;
    (failed.is_empty() && !a.is_null(0)).then_some(a)
}

fn broadcast(v: &ArrayRef, n: usize) -> Result<ArrayRef, Error> {
    let zeros = arrow_array::UInt32Array::from(vec![0u32; n]);
    Ok(arrow_select::take::take(v.as_ref(), &zeros, None)?)
}

/// The stream's schema as the destination gets it: each column held whole
/// at the type the destination has, or left out.
fn deliver_schema(current: &Schema, columns: &BTreeMap<String, Holding>) -> Schema {
    let by_dest: HashMap<&str, &Holding> = columns.values().map(|c| (c.dest.as_str(), c)).collect();
    let fields: Vec<Field> = current
        .fields()
        .iter()
        .filter_map(|f| match by_dest.get(f.name().as_str()) {
            None => Some(f.as_ref().clone()),
            Some(h) => h.delivered.as_ref().map(|d| {
                let mut md = d.metadata().clone();
                if let Some(v) = f.metadata().get(HELD_KEY) {
                    md.insert(HELD_KEY.to_owned(), v.clone());
                }
                d.clone()
                    .with_name(f.name())
                    .with_nullable(true)
                    .with_metadata(md)
            }),
        })
        .collect();
    Schema::new_with_metadata(fields, current.metadata().clone())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::hold::{CELL, COLUMN};
    use arrow_array::Int64Array;
    use arrow_array::cast::AsArray;

    fn names(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn identity() -> Identity {
        Identity {
            stream: "users".into(),
            read_start: 42,
            ..Default::default()
        }
    }

    fn shaper(restored: Option<SchemaRef>) -> Shaper {
        with_policy(Policy::AllowAll, restored)
    }

    fn with_policy(policy: Policy, restored: Option<SchemaRef>) -> Shaper {
        Shaper::new(
            Naming::default(),
            "users".into(),
            identity(),
            policy,
            restored,
            None,
            TypeOptions::default(),
        )
        .unwrap()
    }

    fn rows(s: &mut Shaper, rows: &[&str]) -> Shaped {
        s.rows(rows.iter().map(|r| r.as_bytes())).unwrap()
    }

    fn ids(batch: &RecordBatch) -> Vec<Vec<u8>> {
        let col = batch.column(batch.schema().index_of("_openetl_id").unwrap());
        col.as_fixed_size_binary()
            .value_data()
            .chunks(16)
            .map(<[u8]>::to_vec)
            .collect()
    }

    const META: [&str; 4] = [
        "_openetl_id",
        "_openetl_op",
        "_openetl_seq",
        "_openetl_synced_at",
    ];

    #[test]
    fn names_raw_columns_and_adds_metadata_columns() {
        let mut s = shaper(None);
        assert!(s.schema().is_none());
        let n = rows(&mut s, &[r#"{"Id": 1, "id": "x", "First Name": true}"#]);
        assert!(n.schema_changed);
        let schema = n.batch.schema();
        let mut want = vec!["id", "id_2", "first_name"];
        want.extend(META);
        assert_eq!(names(&schema), want);
        assert_eq!(schema.metadata().get(TABLE_KEY).unwrap(), "users");
        assert_eq!(s.schema().unwrap(), schema);
        let seq = n
            .batch
            .column(5)
            .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
        assert_eq!(seq.values()[0], 42);
        assert_eq!(
            n.batch.column(4).as_string::<i32>().iter().next(),
            Some(Some("insert"))
        );

        let n = rows(&mut s, &[r#"{"First Name": false}"#]);
        assert!(!n.schema_changed);
        assert_eq!(n.batch.schema(), schema);
    }

    /// A Flink job is deployed again at the schema its rows reached, and reads
    /// nothing else, so a shaper started from it must report that schema
    /// unchanged: for a primer of the declared columns, and for the same rows.
    #[test]
    fn a_schema_rows_reached_is_kept_by_a_primer_and_by_the_same_rows() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("_offset", DataType::Int64, false),
            Field::new(
                "_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("_key", DataType::Utf8, true),
            Field::new("_headers", DataType::Utf8, true).with_metadata(HashMap::from([(
                EXTENSION_NAME_KEY.to_owned(),
                "arrow.json".to_owned(),
            )])),
        ]));
        let make = |restored| {
            Shaper::new(
                Naming::default(),
                "t".into(),
                Identity {
                    cursor: Some("_offset".into()),
                    ..identity()
                },
                Policy::AllowAll,
                restored,
                Some(declared.clone()),
                TypeOptions::default(),
            )
            .unwrap()
        };
        let row = r#"{"id": 1, "v": 1, "pad": "ab", "_offset": 0,
            "_at": "2026-11-09T16:26:10Z", "_key": "1", "_headers": null}"#;

        let mut first = make(None);
        let primer = first
            .batch(&RecordBatch::new_empty(declared.clone()))
            .unwrap();
        assert!(primer.schema_changed);
        let reached = rows(&mut first, &[row]);
        assert!(reached.schema_changed);
        let reached = reached.batch.schema();
        assert_ne!(reached, primer.batch.schema());

        let mut again = make(Some(reached.clone()));
        let primer = again
            .batch(&RecordBatch::new_empty(declared.clone()))
            .unwrap();
        assert!(!primer.schema_changed);
        let n = rows(&mut again, &[row]);
        assert!(!n.schema_changed);
        assert_eq!(n.batch.schema(), reached);
    }

    #[test]
    fn restored_names_and_ids_never_change() {
        let mut first = shaper(None);
        let a = rows(&mut first, &[r#"{"foobar": 1}"#, r#"{"FooBar": 2.5}"#]);
        let restored = first.schema().unwrap();
        assert_eq!(names(&restored)[..2], ["foobar", "foobar_2"]);

        // In a new attempt, FooBar arrives first, and keeps its name and type.
        let mut s = shaper(Some(restored.clone()));
        assert_eq!(s.schema().unwrap(), restored);
        let n = rows(
            &mut s,
            &[r#"{"FooBar": 3, "FOOBAR": "x"}"#, r#"{"foobar": 1}"#],
        );
        let schema = n.batch.schema();
        assert_eq!(names(&schema)[..3], ["foobar", "foobar_2", "foobar_3"]);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(
            ids(&n.batch)[1],
            ids(&a.batch)[0],
            "the same row, the same ID"
        );
    }

    #[test]
    fn declared_timestamps_keep_nanoseconds_if_asked() {
        let declared = Schema::new(vec![Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]);
        let mut s = Shaper::new(
            Naming::default(),
            "t".into(),
            identity(),
            Policy::AllowAll,
            None,
            Some(Arc::new(declared)),
            TypeOptions {
                nanoseconds: true,
                ..Default::default()
            },
        )
        .unwrap();
        let n = rows(&mut s, &[r#"{"at": "2000-01-01T00:00:00.123456789Z"}"#]);
        let at = n.batch.column(0);
        assert_eq!(
            at.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            at.as_primitive::<arrow_array::types::TimestampNanosecondType>()
                .values()[0],
            946_684_800_123_456_789
        );
    }

    #[test]
    fn soft_deletes_add_a_deleted_column_from_the_op() {
        let mut s = shaper(None).with_soft_deletes(true);
        let n = rows(&mut s, &[r#"{"id": 1}"#, r#"{"id": 2}"#]);
        let schema = n.batch.schema();
        let mut want = vec!["id"];
        want.extend(META);
        want.push("_openetl_deleted");
        assert_eq!(names(&schema), want);
        let deleted = schema.index_of("_openetl_deleted").unwrap();
        assert!(!schema.field(deleted).is_nullable());
        assert_eq!(
            n.batch
                .column(deleted)
                .as_boolean()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(false), Some(false)]
        );
        assert_eq!(s.schema().unwrap(), schema);
    }

    #[test]
    fn the_cursor_is_the_sequence_once_it_has_a_column() {
        let mut s = Shaper::new(
            Naming::default(),
            "t".into(),
            Identity {
                stream: "t".into(),
                key: vec!["id".into()],
                cursor: Some("At".into()),
                read_start: 0,
            },
            Policy::AllowAll,
            None,
            None,
            TypeOptions::default(),
        )
        .unwrap();
        let n = rows(&mut s, &[r#"{"id": 1}"#]);
        assert!(!names(&n.batch.schema()).contains(&"_openetl_seq".to_string()));
        let n = rows(&mut s, &[r#"{"id": 1, "At": 7}"#]);
        assert!(n.schema_changed);
        let schema = n.batch.schema();
        let seq = schema.index_of("_openetl_seq").unwrap();
        assert_eq!(schema.field(seq).data_type(), &DataType::Int64);
        assert_eq!(
            n.batch.column(seq),
            n.batch.column(schema.index_of("at").unwrap())
        );
    }

    #[test]
    fn renames_typed_batches() {
        let mut s = shaper(None);
        let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let shaped = s.batch(&batch).unwrap();
        assert!(shaped.schema_changed);
        let out = shaped.batch;
        let mut want = vec!["id"];
        want.extend(META);
        assert_eq!(names(&out.schema()), want);
        assert_eq!(out.column(0), batch.column(0));
        assert_eq!(s.schema().unwrap(), out.schema());
        let again = s.batch(&batch).unwrap();
        assert!(!again.schema_changed);
        assert_eq!(again.batch.schema(), out.schema());
        assert_ne!(ids(&out)[0], ids(&out)[1]);
    }

    fn change_batch(ops: &[Option<&str>], seqs: &[i64], doc: Vec<Option<&str>>) -> RecordBatch {
        use crate::control::CONTROL_KEY;
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let control = |name: &str, dt: DataType, c: &str| {
            Field::new(name, dt, true)
                .with_metadata(HashMap::from([(CONTROL_KEY.to_owned(), c.to_owned())]))
        };
        let mut unchanged = ListBuilder::new(StringBuilder::new());
        for d in &doc {
            if d.is_none() {
                unchanged.values().append_value("Doc");
                unchanged.values().append_value("gone");
            }
            unchanged.append(true);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("Doc", DataType::Utf8, true),
            // A record field may share a control column's name.
            control("id", DataType::Utf8, "op"),
            control("seq", DataType::Int64, "seq"),
            control(
                "unchanged",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                "unchanged",
            ),
        ]));
        let n = seqs.len() as i64;
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..n)),
                Arc::new(StringArray::from(doc)),
                Arc::new(StringArray::from(ops.to_vec())),
                Arc::new(Int64Array::from(seqs.to_vec())),
                Arc::new(unchanged.finish()),
            ],
        )
        .unwrap()
    }

    fn changelog_shaper(declared: Option<SchemaRef>) -> Shaper {
        Shaper::new(
            Naming::default(),
            "t".into(),
            Identity {
                stream: "t".into(),
                key: vec!["id".into()],
                ..Default::default()
            },
            Policy::AllowAll,
            None,
            declared,
            TypeOptions::default(),
        )
        .unwrap()
        .with_soft_deletes(true)
        .with_changelog(true)
    }

    #[test]
    fn a_changelog_takes_op_seq_and_unchanged_from_control_columns() {
        let mut s = changelog_shaper(None);
        let b = change_batch(
            &[None, Some("update"), Some("delete")],
            &[10, 20, 30],
            vec![Some("a"), None, Some("c")],
        );
        let n = s.batch(&b).unwrap();
        let schema = n.batch.schema();
        assert_eq!(
            names(&schema),
            [
                "id",
                "doc",
                "_openetl_id",
                "_openetl_op",
                "_openetl_seq",
                "_openetl_synced_at",
                "_openetl_deleted",
                "_openetl_unchanged"
            ]
        );
        let col = |name: &str| n.batch.column(schema.index_of(name).unwrap()).clone();
        assert_eq!(
            col("_openetl_op")
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            [Some("insert"), Some("update"), Some("delete")]
        );
        assert_eq!(
            schema.field_with_name("_openetl_seq").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            col("_openetl_seq")
                .as_primitive::<arrow_array::types::Int64Type>()
                .values(),
            &[10, 20, 30]
        );
        assert_eq!(
            col("_openetl_deleted")
                .as_boolean()
                .iter()
                .collect::<Vec<_>>(),
            [Some(false), Some(false), Some(true)]
        );
        let unchanged: Vec<Vec<Option<String>>> = col("_openetl_unchanged")
            .as_list::<i32>()
            .iter()
            .map(|l| {
                l.unwrap()
                    .as_string::<i32>()
                    .iter()
                    .map(|v| v.map(str::to_owned))
                    .collect()
            })
            .collect();
        // Under its destination name; a column the batch doesn't have is
        // left out.
        assert_eq!(unchanged, [vec![], vec![Some("doc".to_owned())], vec![]]);

        // The sequence is part of the ID.
        let again = s
            .batch(&change_batch(&[None], &[11], vec![Some("a")]))
            .unwrap();
        assert!(!again.schema_changed);
        assert_ne!(ids(&again.batch)[0], ids(&n.batch)[0]);
        let same = s
            .batch(&change_batch(&[None], &[10], vec![Some("x")]))
            .unwrap();
        assert_eq!(ids(&same.batch)[0], ids(&n.batch)[0]);
    }

    #[test]
    fn a_changelog_batch_needs_op_and_seq() {
        let mut s = changelog_shaper(None);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(matches!(s.batch(&b), Err(Error::Declared(_))));

        let mut b = change_batch(&[Some("upsert")], &[1], vec![Some("a")]);
        assert!(matches!(s.batch(&b), Err(Error::Declared(_))));
        b = change_batch(&[None], &[1], vec![Some("a")]);
        assert!(s.batch(&b).is_ok());
    }

    #[test]
    fn rejected_rows_take_their_control_values_with_them() {
        // doc is declared an integer, so "x" is quarantined.
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("Doc", DataType::Int64, true),
        ]));
        let mut s = changelog_shaper(Some(declared));
        let b = change_batch(
            &[Some("insert"), Some("update"), Some("delete")],
            &[10, 20, 30],
            vec![Some("1"), Some("x"), Some("3")],
        );
        let n = s.batch(&b).unwrap();
        assert_eq!(n.rejected.len(), 1);
        assert_eq!(n.rejected[0].row, 1);
        let schema = n.batch.schema();
        let col = |name: &str| n.batch.column(schema.index_of(name).unwrap()).clone();
        assert_eq!(
            col("_openetl_op")
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            [Some("insert"), Some("delete")]
        );
        assert_eq!(
            col("_openetl_seq")
                .as_primitive::<arrow_array::types::Int64Type>()
                .values(),
            &[10, 30]
        );
    }

    fn data_names(schema: &Schema) -> Vec<String> {
        names(schema)
            .into_iter()
            .filter(|n| !n.starts_with("_openetl_"))
            .collect()
    }

    #[test]
    fn policies_apply_once_the_schema_is_established() {
        let mut first = shaper(None);
        rows(&mut first, &[r#"{"x": 1}"#]);
        let restored = first.schema();

        let mut s = with_policy(Policy::IgnoreNew, restored.clone());
        let n = rows(&mut s, &[r#"{"x": 2, "y": "new"}"#, r#"{"x": 2.5}"#]);
        assert!(!n.schema_changed);
        assert_eq!(data_names(&n.batch.schema()), ["x"]);
        assert_eq!(n.batch.num_rows(), 1);
        assert_eq!(n.rejected.len(), 1);
        assert_eq!(n.rejected[0].row, 1);
        assert!(n.rejected[0].reason.starts_with("type: column \"x\""));

        let mut s = with_policy(Policy::Block, restored);
        let err = s.rows([br#"{"x": 1, "y": 1}"#.as_slice()]).unwrap_err();
        assert!(matches!(err, Error::Drift(_)), "{err}");

        // A new stream takes its first epoch's schema as it comes.
        let mut s = with_policy(Policy::Block, None);
        rows(&mut s, &[r#"{"x": 1}"#, r#"{"x": 1.5, "y": true}"#]);
        s.seal();
        assert!(s.rows([br#"{"z": 1}"#.as_slice()]).is_err());
    }

    #[test]
    fn versioned_columns_are_restored() {
        let typed = |a: ArrayRef| RecordBatch::try_from_iter(vec![("X", a)]).unwrap();
        let mut s = shaper(None);
        s.batch(&typed(Arc::new(Int64Array::from(vec![1]))))
            .unwrap();
        s.seal();
        let flag = typed(Arc::new(arrow_array::BooleanArray::from(vec![true])));
        let n = s.batch(&flag).unwrap();
        assert!(n.schema_changed);
        assert_eq!(n.versioned.len(), 1);
        let schema = n.batch.schema();
        assert_eq!(data_names(&schema), ["x", "x__v2"]);
        assert_eq!(schema.metadata().get(VERSIONS_KEY).unwrap(), r#"{"X":2}"#);

        let mut r = shaper(Some(schema.clone()));
        let n = r.batch(&flag).unwrap();
        assert_eq!(n.batch.schema().fields(), schema.fields());
        assert_eq!(n.batch.column(0).null_count(), 1);
        assert!(n.versioned.is_empty());
    }

    #[test]
    fn a_restored_stream_remembers_columns_past_2_53() {
        let typed = |a: ArrayRef| RecordBatch::try_from_iter(vec![("X", a)]).unwrap();
        let mut s = shaper(None);
        let n = s
            .batch(&typed(Arc::new(Int64Array::from(vec![(1 << 53) + 1]))))
            .unwrap();
        s.seal();
        let schema = n.batch.schema();
        assert_eq!(schema.metadata().get(BIG_INTS_KEY).unwrap(), r#"["X"]"#);

        let mut r = shaper(Some(schema.clone()));
        let float = typed(Arc::new(arrow_array::Float64Array::from(vec![1.5])));
        let n = r.batch(&float).unwrap();
        assert!(n.schema_changed);
        assert_eq!(n.batch.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn a_first_big_integer_is_not_a_schema_change() {
        let typed = |a: ArrayRef| RecordBatch::try_from_iter(vec![("X", a)]).unwrap();
        let mut s = shaper(None);
        let first = s
            .batch(&typed(Arc::new(Int64Array::from(vec![1]))))
            .unwrap();
        assert!(first.schema_changed);
        let big = s
            .batch(&typed(Arc::new(Int64Array::from(vec![(1 << 53) + 1]))))
            .unwrap();
        assert!(!big.schema_changed);
        assert_eq!(
            big.batch.schema().metadata().get(BIG_INTS_KEY).unwrap(),
            r#"["X"]"#
        );
    }

    #[test]
    fn rejects_a_restored_schema_with_a_repeated_name() {
        let bad = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("x", DataType::Int64, true),
                Field::new("x_2", DataType::Utf8, true),
            ],
            HashMap::from([(SOURCE_NAMES_KEY.to_owned(), r#"{"x_2":"x"}"#.to_owned())]),
        ));
        let err = Shaper::new(
            Naming::default(),
            "t".into(),
            identity(),
            Policy::AllowAll,
            Some(bad),
            None,
            TypeOptions::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Restored(_)), "{err}");
    }

    fn keyed(policy: Policy, restored: Option<SchemaRef>, declared: Option<SchemaRef>) -> Shaper {
        Shaper::new(
            Naming::default(),
            "t".into(),
            Identity {
                stream: "t".into(),
                key: vec!["id".into()],
                ..Default::default()
            },
            policy,
            restored,
            declared,
            TypeOptions::default(),
        )
        .unwrap()
    }

    fn held_lists(b: &RecordBatch) -> Vec<Option<Vec<String>>> {
        b.column(b.schema().index_of(HELD_COLUMN).unwrap())
            .as_list::<i32>()
            .iter()
            .map(|l| {
                l.map(|l| {
                    l.as_string::<i32>()
                        .iter()
                        .map(|v| v.unwrap().to_owned())
                        .collect()
                })
            })
            .collect()
    }

    fn typed(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(cols).unwrap()
    }

    fn price_shaper(config: HoldConfig) -> Shaper {
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
        ]));
        keyed(Policy::AllowAll, None, Some(declared))
            .with_holds(config)
            .unwrap()
    }

    fn prices() -> RecordBatch {
        typed(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("price", Arc::new(StringArray::from(vec!["1.5", "NaN"]))),
        ])
    }

    #[test]
    fn a_value_that_doesnt_fit_holds_its_cell() {
        let mut plain = price_shaper(HoldConfig::default());
        let without = plain.batch(&prices()).unwrap();
        assert_eq!(without.rejected.len(), 1);

        let mut s = price_shaper(HoldConfig {
            cells: true,
            ..Default::default()
        });
        let n = s.batch(&prices()).unwrap();
        assert!(n.rejected.is_empty());
        assert!(n.schema_changed);
        assert_eq!(n.batch.num_rows(), 2);
        let price = n.batch.column(n.batch.schema().index_of("price").unwrap());
        assert!(price.is_null(1));
        assert_eq!(held_lists(&n.batch), [None, Some(vec!["price".into()])]);
        assert_eq!(
            ids(&n.batch)[0],
            ids(&without.batch)[0],
            "a hold changes no ID"
        );
        assert_eq!(n.cells.len(), 1);
        assert!(n.cells[0].1.starts_with("nan:"), "{:?}", n.cells);

        assert_eq!(n.held.len(), 1);
        let h = &n.held[0];
        assert_eq!(h.num_rows(), 1);
        assert_eq!(
            names(&h.schema()),
            [
                "id",
                "_openetl_id",
                "_openetl_seq",
                "_openetl_op",
                HELD_COLUMN,
                "price"
            ]
        );
        let f = h.schema().field_with_name("price").unwrap().clone();
        assert_eq!(
            f.metadata().get(HOLD_KIND_KEY).map(String::as_str),
            Some(CELL)
        );
        assert_eq!(
            h.column(5).as_string::<i32>().iter().next(),
            Some(Some("NaN"))
        );
        assert!(s.schema().unwrap().field_with_name(HELD_COLUMN).is_ok());
        assert!(s.delivered().is_none(), "cells don't change the schema");
    }

    #[test]
    fn a_held_cell_can_show_a_value() {
        let mut s = price_shaper(HoldConfig {
            cells: true,
            columns: HashMap::from([("price".into(), Show::Value("0".into()))]),
            ..Default::default()
        });
        let n = s.batch(&prices()).unwrap();
        let price = n.batch.column(n.batch.schema().index_of("price").unwrap());
        let price = price.as_primitive::<arrow_array::types::Decimal128Type>();
        assert_eq!(price.iter().nth(1), Some(Some(0)));
        assert_eq!(held_lists(&n.batch)[1], Some(vec!["price".into()]));
    }

    #[test]
    fn declared_types_reach_the_schema_before_any_row() {
        let mut first = price_shaper(HoldConfig::default());
        first.batch(&prices()).unwrap();
        let wider = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Utf8, true),
        ]));
        let mut s = keyed(Policy::AllowAll, first.schema(), Some(wider));
        assert!(s.declare().unwrap());
        let schema = s.schema().unwrap();
        assert_eq!(
            schema.field_with_name("price").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("_openetl_seq").unwrap().data_type(),
            first
                .schema()
                .unwrap()
                .field_with_name("_openetl_seq")
                .unwrap()
                .data_type()
        );
        assert!(!s.declare().unwrap(), "nothing left to take");
        let mut fresh = keyed(Policy::AllowAll, None, None);
        assert!(!fresh.declare().unwrap());
    }

    #[test]
    fn a_key_never_holds_a_cell() {
        let declared = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let mut s = keyed(Policy::AllowAll, None, Some(declared))
            .with_holds(HoldConfig {
                cells: true,
                ..Default::default()
            })
            .unwrap();
        let b = typed(vec![(
            "id",
            Arc::new(StringArray::from(vec!["1", "x"])) as ArrayRef,
        )]);
        let n = s.batch(&b).unwrap();
        assert_eq!(n.rejected.len(), 1);
        assert!(n.held.is_empty());
    }

    fn established(policy: Policy, config: HoldConfig) -> Shaper {
        let mut first = keyed(Policy::AllowAll, None, None);
        first
            .batch(&typed(vec![
                ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ("x", Arc::new(Int64Array::from(vec![10]))),
            ]))
            .unwrap();
        keyed(policy, first.schema(), None)
            .with_holds(config)
            .unwrap()
    }

    #[test]
    fn block_holds_a_new_column() {
        let mut s = established(Policy::Block, HoldConfig::default());
        let b = typed(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("x", Arc::new(Int64Array::from(vec![11, 12]))),
            ("y", Arc::new(StringArray::from(vec![Some("a"), None]))),
        ]);
        let n = s.batch(&b).unwrap();
        assert_eq!(data_names(&n.batch.schema()), ["id", "x"]);
        assert_eq!(
            held_lists(&n.batch),
            [Some(vec!["y".into()]), Some(vec!["y".into()])]
        );
        assert_eq!(data_names(&s.schema().unwrap()), ["id", "x", "y"]);
        assert_eq!(s.delivered().unwrap().fields(), n.batch.schema().fields());
        let holding = s.holding();
        assert_eq!(holding.len(), 1);
        assert_eq!(
            (holding[0].column.as_str(), holding[0].delivered.is_none()),
            ("y", true)
        );
        assert!(holding[0].reason.contains("block"), "{}", holding[0].reason);
        assert_eq!(holding[0].why, Why::Policy("block".into()));
        assert_eq!(holding[0].source, "y");

        let h = &n.held[0];
        assert_eq!(h.num_rows(), 2);
        let y = h.schema().field_with_name("y").unwrap().clone();
        assert_eq!(
            y.metadata().get(HOLD_KIND_KEY).map(String::as_str),
            Some(COLUMN)
        );

        // Held whole, it gets no patches; released, its values become
        // patches.
        let (none, still) = s.patch(h, &HashSet::from(["y".to_owned()])).unwrap();
        assert!(none.is_none());
        assert_eq!(still, 0);
        let s = keyed(Policy::Block, s.schema(), None)
            .with_holds(HoldConfig::default())
            .unwrap();
        let (p, still) = s.patch(h, &HashSet::from(["y".to_owned()])).unwrap();
        let p = p.unwrap();
        assert_eq!(still, 0);
        assert_eq!(
            names(&p.schema()),
            [
                "id",
                "_openetl_id",
                "_openetl_seq",
                "_openetl_synced_at",
                HELD_COLUMN,
                "y"
            ]
        );
        assert_eq!(p.num_rows(), 2);
        assert_eq!(held_lists(&p)[0], Some(vec!["y".into()]));
        assert_eq!(
            p.schema().metadata().get(TABLE_KEY).map(String::as_str),
            Some("t")
        );
        assert_eq!(
            p.column(5).as_string::<i32>().iter().next(),
            Some(Some("a"))
        );
        assert!(p.column(5).is_null(1), "a held null is patched as null");
        let (none, _) = s.patch(h, &HashSet::from(["z".to_owned()])).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn allow_columns_holds_a_change_that_isnt_a_widening() {
        let mut s = established(Policy::AllowColumns, HoldConfig::default());
        let b = typed(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("x", Arc::new(arrow_array::BooleanArray::from(vec![true]))),
        ]);
        let n = s.batch(&b).unwrap();
        let x = n.batch.schema().field_with_name("x").unwrap().clone();
        assert_eq!(x.data_type(), &DataType::Int64, "the destination's type");
        assert!(
            n.batch
                .column(n.batch.schema().index_of("x").unwrap())
                .is_null(0)
        );
        assert_eq!(
            s.schema()
                .unwrap()
                .field_with_name("x")
                .unwrap()
                .data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            n.held[0].schema().field_with_name("x").unwrap().data_type(),
            &DataType::Boolean
        );

        // A restored hold goes on holding, with nothing to decide.
        let mut r = keyed(Policy::AllowColumns, s.schema(), None)
            .with_holds(HoldConfig {
                holding: s.holding(),
                ..Default::default()
            })
            .unwrap();
        let n = r.batch(&b).unwrap();
        assert_eq!(
            n.batch.schema().field_with_name("x").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(held_lists(&n.batch), [Some(vec!["x".into()])]);
    }

    #[test]
    fn a_fixed_column_holds_a_widening() {
        let config = HoldConfig {
            limits: crate::hold::Limits {
                fixed: HashSet::from(["x".to_owned()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut s = established(Policy::AllowAll, config);
        let b = typed(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("x", Arc::new(arrow_array::Float64Array::from(vec![1.5]))),
        ]);
        let n = s.batch(&b).unwrap();
        assert_eq!(
            n.batch.schema().field_with_name("x").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(
            s.holding()[0]
                .reason
                .contains("changed from int64 to float64, which the destination can't change"),
            "{:?}",
            s.holding()
        );
        assert_eq!(s.holding()[0].why, Why::Fixed);
    }

    #[test]
    fn a_release_to_a_new_version_patches_it() {
        let fixed = || HoldConfig {
            limits: crate::hold::Limits {
                fixed: HashSet::from(["x".to_owned()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut s = established(Policy::AllowAll, fixed());
        let n = s
            .batch(&typed(vec![
                ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ("x", Arc::new(arrow_array::Float64Array::from(vec![1.5]))),
            ]))
            .unwrap();
        let held = n.held[0].clone();
        let kept = ColumnHold {
            column: "x".into(),
            delivered: Some(Field::new("x", DataType::Int64, true)),
            reason: "fixed".into(),
            why: Why::Fixed,
            source: String::new(),
        };
        let released = HashSet::from(["x".to_owned()]);
        let restart = |restored: Option<SchemaRef>| {
            let mut s = keyed(Policy::AllowAll, restored, None)
                .with_holds(fixed())
                .unwrap();
            let changed = s.version(std::slice::from_ref(&kept)).unwrap();
            (s, changed)
        };
        let (mut s, changed) = restart(s.schema());
        assert!(changed);
        let schema = s.schema().unwrap();
        assert_eq!(data_names(&schema), ["id", "x", "x__v2"]);
        assert_eq!(
            schema.field_with_name("x").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("x__v2").unwrap().data_type(),
            &DataType::Float64
        );

        let check = |s: &Shaper| {
            let (p, still) = s.patch(&held, &released).unwrap();
            let p = p.unwrap();
            assert_eq!(still, 0);
            let f = p.schema().field_with_name("x__v2").unwrap().clone();
            assert_eq!(f.metadata().get(FILLS_KEY).map(String::as_str), Some("x"));
            assert!(p.schema().field_with_name("x").is_err());
            let v = p
                .column_by_name("x__v2")
                .unwrap()
                .as_primitive::<arrow_array::types::Float64Type>();
            assert_eq!(v.iter().collect::<Vec<_>>(), [Some(1.5)]);
            let lists = string_lists(p.column_by_name(HELD_COLUMN).unwrap());
            assert_eq!(lists[0].as_deref(), Some(&["x".to_owned()][..]));
        };
        check(&s);

        let n = s
            .batch(&typed(vec![
                ("id", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
                ("x", Arc::new(arrow_array::Float64Array::from(vec![2.5]))),
            ]))
            .unwrap();
        assert!(n.held.is_empty() && s.holding().is_empty());
        assert!(n.batch.column_by_name("x").unwrap().is_null(0));
        assert!(!n.batch.column_by_name("x__v2").unwrap().is_null(0));

        // The next attempt restores the version, and adds no other.
        let (s, changed) = restart(s.schema());
        assert!(!changed);
        assert_eq!(data_names(&s.schema().unwrap()), ["id", "x", "x__v2"]);
        check(&s);
    }

    #[test]
    fn grouped_columns_are_held_together() {
        let mut first = keyed(Policy::AllowAll, None, None);
        first
            .batch(&typed(vec![
                ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ("amount", Arc::new(Int64Array::from(vec![10]))),
                ("currency", Arc::new(StringArray::from(vec!["EUR"]))),
            ]))
            .unwrap();
        let mut s = keyed(Policy::AllowColumns, first.schema(), None)
            .with_holds(HoldConfig {
                groups: vec![vec!["amount".into(), "currency".into()]],
                ..Default::default()
            })
            .unwrap();
        let n = s
            .batch(&typed(vec![
                ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                (
                    "amount",
                    Arc::new(arrow_array::BooleanArray::from(vec![true])),
                ),
                ("currency", Arc::new(StringArray::from(vec!["USD"]))),
            ]))
            .unwrap();
        let mut held: Vec<(String, Why)> =
            s.holding().into_iter().map(|h| (h.column, h.why)).collect();
        held.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            held,
            [
                ("amount".into(), Why::Policy("allow_columns".into())),
                ("currency".into(), Why::Group("amount".into()))
            ]
        );
        assert_eq!(
            held_lists(&n.batch),
            [Some(vec!["amount".into(), "currency".into()])]
        );
        assert!(
            n.batch
                .column(n.batch.schema().index_of("currency").unwrap())
                .is_null(0)
        );
    }

    #[test]
    fn a_column_that_cant_be_held_still_stops() {
        let config = HoldConfig {
            limits: crate::hold::Limits {
                not_null: HashSet::from(["x".to_owned()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut s = established(Policy::AllowColumns, config);
        let b = typed(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("x", Arc::new(arrow_array::BooleanArray::from(vec![true]))),
        ]);
        assert!(matches!(s.batch(&b), Err(Error::Drift(_))));
    }

    #[test]
    fn a_column_an_update_didnt_send_isnt_held_in_its_row() {
        let mut first = changelog_shaper(None);
        first
            .batch(&change_batch(&[None], &[1], vec![Some("a")]))
            .unwrap();
        let mut s = Shaper::new(
            Naming::default(),
            "t".into(),
            Identity {
                stream: "t".into(),
                key: vec!["id".into()],
                ..Default::default()
            },
            Policy::AllowColumns,
            first.schema(),
            None,
            TypeOptions::default(),
        )
        .unwrap()
        .with_soft_deletes(true)
        .with_changelog(true)
        .with_holds(HoldConfig {
            holding: vec![ColumnHold {
                column: "doc".into(),
                delivered: Some(Field::new("doc", DataType::Utf8, true)),
                reason: "test".into(),
                why: Why::Refused,
                source: String::new(),
            }],
            ..Default::default()
        })
        .unwrap();
        let n = s
            .batch(&change_batch(
                &[None, Some("update")],
                &[2, 3],
                vec![Some("b"), None],
            ))
            .unwrap();
        assert_eq!(held_lists(&n.batch), [Some(vec!["doc".into()]), None]);
        assert_eq!(n.held[0].num_rows(), 1);
    }
}
