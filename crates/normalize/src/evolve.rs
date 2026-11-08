//! A stream's schema across batches and attempts. Types only widen, and each
//! batch is conformed to the stream's schema: columns in the schema's order,
//! narrower types cast up, missing columns null. The schema policy decides
//! which changes to an established schema are taken.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::hold::{HeldCell, Why};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, new_null_array};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema};

use crate::coerce::{self, Failures, Infinity, Target};
use crate::{
    EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, Error, JSON_EXTENSION, MAX_EXACT_FLOAT_INT,
    logical_type,
};

/// Schema metadata: a JSON object from each source column whose type changed
/// incompatibly to its current version, 2 or more. Version k's values are in
/// the column `{name}__v{k}`.
pub const VERSIONS_KEY: &str = "openetl.versions";

/// Schema metadata: a JSON array of the columns, by their names in the
/// schema, that have held an `int64` value past ±2^53. `float64` would round
/// them, so such a column mixed with `float64` becomes `string`.
pub const BIG_INTS_KEY: &str = "openetl.big_ints";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Policy {
    #[default]
    AllowAll,
    AllowColumns,
    Block,
    IgnoreNew,
}

/// Whether `to` holds every value of `from`. `int64` to `float64` counts, as
/// it does for inference, within ±2^53; [`Evolver::conform`] looks at the
/// values and sends a column past that to `string`.
pub fn widens(from: &Field, to: &Field) -> bool {
    use DataType as D;
    let (a, b) = (from.data_type(), to.data_type());
    let json = |f: &Field| {
        f.metadata().get(EXTENSION_NAME_KEY).map(String::as_str) == Some(JSON_EXTENSION)
    };
    if a == b {
        return json(from) == json(to) || !json(to);
    }
    let int_digits = |dt: &D| match dt {
        D::Int16 => Some(5),
        D::Int32 => Some(10),
        D::Int64 => Some(19),
        _ => None,
    };
    match (a, b) {
        (_, D::Utf8) => !json(to),
        (D::Int16, D::Int32 | D::Int64 | D::Float32 | D::Float64) => true,
        (D::Int32, D::Int64 | D::Float64) => true,
        (D::Int64, D::Float64) | (D::Float32, D::Float64) => true,
        (D::Int16 | D::Int32 | D::Int64, D::Decimal128(p, s)) => {
            int_digits(a).is_some_and(|n| i16::from(*p) - i16::from(*s) >= n)
        }
        (D::Decimal128(p1, s1), D::Decimal128(p2, s2)) => {
            s2 >= s1 && i16::from(*p2) - i16::from(*s2) >= i16::from(*p1) - i16::from(*s1)
        }
        _ => false,
    }
}

/// A batch conformed to the stream's schema.
#[derive(Debug)]
pub(crate) struct Conformed {
    pub batch: RecordBatch,
    /// Rows left out, by their index in the input batch.
    pub failed: Failures,
    pub changed: bool,
    /// What changed incompatibly, and where its values go now.
    pub versioned: Vec<String>,
    /// Cells held instead of failing their rows, by row of `batch`.
    pub cells: Vec<HeldCell>,
    /// Columns this batch started holding, with why.
    pub held: Vec<(String, String, Why)>,
    /// A column held its first value past ±2^53, which the schema's metadata
    /// records though no column changed.
    pub big_ints_grew: bool,
}

/// When the evolver holds a column instead of stopping on its change, or
/// sending one the destination can't take. Columns are named by their
/// source names.
#[derive(Clone, Debug, Default)]
pub(crate) struct HoldRules {
    pub unholdable: HashSet<String>,
    /// Columns whose type the destination can't change.
    pub fixed: HashSet<String>,
    pub groups: Vec<Vec<String>>,
    /// New columns the destination still takes; None for no limit.
    pub columns_left: Option<usize>,
    /// A value ignore_new can't coerce holds its cell.
    pub cells: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct Evolver {
    policy: Policy,
    /// Under source names; a column versioned k times is `{name}__v{k}`.
    fields: Vec<Field>,
    index: HashMap<String, usize>,
    versions: BTreeMap<String, u32>,
    established: bool,
    /// For values coerced to an established date or timestamp.
    pub infinity: Infinity,
    pub rules: Option<HoldRules>,
    /// Columns held whole, by their names in the schema: their values pass
    /// through, and their type follows them.
    pub holding: HashSet<String>,
    /// Columns that have held an `int64` value past ±2^53, by their names in
    /// the schema. [`BIG_INTS_KEY`] carries them across attempts.
    pub big_ints: BTreeSet<String>,
}

impl Evolver {
    /// `restored` holds the source columns of a schema the stream had, and
    /// `versions` its versioned columns.
    pub fn new(policy: Policy, restored: Option<&Schema>, versions: BTreeMap<String, u32>) -> Self {
        let mut e = Self {
            policy,
            fields: Vec::new(),
            index: HashMap::new(),
            versions,
            established: false,
            infinity: Infinity::default(),
            rules: None,
            holding: HashSet::new(),
            big_ints: BTreeSet::new(),
        };
        for f in restored.map(|r| r.fields().iter()).into_iter().flatten() {
            e.add(f.as_ref().clone());
        }
        e.established = !e.fields.is_empty();
        e
    }

    fn add(&mut self, f: Field) {
        self.index.insert(f.name().clone(), self.fields.len());
        self.fields.push(f);
    }

    /// Changes from now on follow the policy.
    pub fn establish(&mut self) {
        self.established = !self.fields.is_empty();
    }

    pub fn established(&self) -> bool {
        self.established
    }

    pub fn versions(&self) -> &BTreeMap<String, u32> {
        &self.versions
    }

    fn current(&self, source: &str) -> String {
        match self.versions.get(source) {
            Some(k) => format!("{source}__v{k}"),
            None => source.to_owned(),
        }
    }

    /// The current version of each source column, under its source name.
    pub fn live(&self) -> Schema {
        let mut superseded = HashSet::new();
        let mut current = HashMap::new();
        for (s, k) in &self.versions {
            superseded.insert(s.clone());
            for j in 2..*k {
                superseded.insert(format!("{s}__v{j}"));
            }
            current.insert(format!("{s}__v{k}"), s.clone());
        }
        let fields: Vec<Field> = self
            .fields
            .iter()
            .filter(|f| !superseded.contains(f.name()))
            .map(|f| match current.get(f.name()) {
                Some(s) => f.clone().with_name(s),
                None => f.clone(),
            })
            .collect();
        Schema::new(fields)
    }

    pub fn schema(&self) -> Schema {
        Schema::new(self.fields.clone())
    }

    /// Holds `name` and the columns grouped with it, if each can be held.
    /// A grouped column's Why::Group names `name` by its source name.
    fn hold(
        &mut self,
        name: &str,
        reason: String,
        why: Why,
        held: &mut Vec<(String, String, Why)>,
    ) -> bool {
        let Some(rules) = &self.rules else {
            return false;
        };
        let group = rules
            .groups
            .iter()
            .find(|g| g.iter().any(|c| c == name))
            .cloned()
            .unwrap_or_else(|| vec![name.to_owned()]);
        if group.iter().any(|c| rules.unholdable.contains(c)) {
            return false;
        }
        for c in group {
            if self.holding.insert(c.clone()) {
                let (reason, why) = match c == name {
                    true => (reason.clone(), why.clone()),
                    false => (format!("held with {name:?}"), Why::Group(name.to_owned())),
                };
                held.push((c, reason, why));
            }
        }
        true
    }

    /// Sends the values of `name`, a column held whole, to a new version
    /// of its source column, as an incompatible change does; `name` takes
    /// `kept`'s type, the one the destination has. Once `name` isn't its
    /// source column's current version, does nothing. The version's name,
    /// or None if the stream has no `name`.
    pub fn version(&mut self, name: &str, kept: &Field) -> Option<String> {
        let i = *self.index.get(name)?;
        let base = self
            .versions
            .keys()
            .find(|s| {
                *s == name
                    || name
                        .strip_prefix(s.as_str())
                        .and_then(|v| v.strip_prefix("__v"))
                        .is_some_and(|k| k.parse::<u32>().is_ok())
            })
            .cloned()
            .unwrap_or_else(|| name.to_owned());
        if self.current(&base) != name {
            return Some(self.current(&base));
        }
        if let Some(left) = self.rules.as_mut().and_then(|r| r.columns_left.as_mut()) {
            *left = left.saturating_sub(1);
        }
        let k = self.versions.get(&base).map_or(2, |k| k + 1);
        self.versions.insert(base.clone(), k);
        let next = self.current(&base);
        let f = self.fields[i].clone().with_name(&next).with_nullable(true);
        self.fields[i] = kept.clone().with_name(name).with_nullable(true);
        self.holding.remove(name);
        self.add(f);
        Some(next)
    }

    fn policy_name(&self) -> &'static str {
        match self.policy {
            Policy::AllowAll => "allow_all",
            Policy::AllowColumns => "allow_columns",
            Policy::Block => "block",
            Policy::IgnoreNew => "ignore_new",
        }
    }

    /// Whether a batch's `f` meets `cur` in `int64` and `float64` on a column
    /// past ±2^53, where `float64` can't hold every value.
    fn mixes_big_ints(&self, name: &str, cur: &Field, f: &Field) -> bool {
        matches!(
            (cur.data_type(), f.data_type()),
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64)
        ) && self.big_ints.contains(name)
    }

    /// A change the policy doesn't take. In a stream that holds, the column
    /// couldn't be held.
    fn drift(&self, what: String) -> Error {
        let mut msg = self.policy_reason(&what);
        if self.rules.is_some() {
            msg.push_str(
                ", and the column can't be held: it's part of the key or cursor, the destination can't hold it, or it can't be null",
            );
        }
        Error::Drift(msg)
    }

    fn policy_reason(&self, what: &str) -> String {
        format!(
            "{what}, which schema policy {} doesn't take",
            self.policy_name()
        )
    }

    /// Conforms a batch, whose columns have source names, to the stream's
    /// schema, taking the changes the policy allows.
    pub fn conform(&mut self, batch: &RecordBatch) -> Result<Conformed, Error> {
        let strict = self.established;
        let big_ints = self.big_ints.len();
        let mut changed = false;
        let mut columns: HashMap<String, ArrayRef> = HashMap::new();
        let mut failed: HashMap<usize, String> = HashMap::new();
        let mut cells = Vec::new();
        let mut held = Vec::new();
        let mut versioned = Vec::new();
        let schema = batch.schema();
        for (f, col) in schema.fields().iter().zip(batch.columns()) {
            let name = self.current(f.name());
            if !self.big_ints.contains(&name) && past_exact_float(col) {
                self.big_ints.insert(name.clone());
            }
            let Some(&i) = self.index.get(&name) else {
                let full = self
                    .rules
                    .as_ref()
                    .is_some_and(|r| r.columns_left == Some(0));
                match self.policy {
                    Policy::IgnoreNew if strict => continue,
                    Policy::Block if strict => {
                        let what = format!("column {:?} is new", f.name());
                        let why = Why::Policy(self.policy_name().to_owned());
                        if !self.hold(&name, self.policy_reason(&what), why, &mut held) {
                            return Err(self.drift(what));
                        }
                    }
                    _ if full => {
                        let reason = format!(
                            "column {:?} is new, and the destination takes no more columns",
                            f.name()
                        );
                        self.hold(&name, reason, Why::Full, &mut held);
                    }
                    _ => {}
                }
                if !self.holding.contains(&name)
                    && let Some(left) = self.rules.as_mut().and_then(|r| r.columns_left.as_mut())
                {
                    *left = left.saturating_sub(1);
                }
                self.add(f.as_ref().clone().with_name(&name).with_nullable(true));
                columns.insert(name, col.clone());
                changed = true;
                continue;
            };
            let cur = self.fields[i].clone();
            let as_text = self.mixes_big_ints(&name, &cur, f);
            let (text_field, text_col);
            let (f, col) = if as_text {
                text_field = Arc::new(Field::new(f.name(), DataType::Utf8, true));
                text_col = cast_up(col, &text_field)?;
                (&text_field, &text_col)
            } else {
                (f, col)
            };
            if cur.data_type() == f.data_type() && widens(f, &cur) {
                columns.insert(name, col.clone());
                continue;
            }
            if widens(f, &cur) {
                columns.insert(name, cast_up(col, &cur)?);
                continue;
            }
            let widening = widens(&cur, f);
            if self.holding.contains(&name) {
                self.fields[i] = match widening {
                    true => widened(&cur, f),
                    false => f.as_ref().clone().with_name(&name).with_nullable(true),
                };
                columns.insert(name, col.clone());
                changed = true;
                continue;
            }
            let what = format!(
                "column {:?} changed from {} to {}",
                f.name(),
                logical_type(&cur),
                logical_type(f)
            );
            let stops = match self.policy {
                Policy::Block => strict,
                Policy::AllowColumns => strict && !widening,
                _ => false,
            };
            let fixed = widening && self.rules.as_ref().is_some_and(|r| r.fixed.contains(&name));
            if stops || fixed {
                let (reason, why) = match stops {
                    true => (
                        self.policy_reason(&what),
                        Why::Policy(self.policy_name().to_owned()),
                    ),
                    false => (
                        format!("{what}, which the destination can't change"),
                        Why::Fixed,
                    ),
                };
                if self.hold(&name, reason, why, &mut held) {
                    self.fields[i] = match widening {
                        true => widened(&cur, f),
                        false => f.as_ref().clone().with_name(&name).with_nullable(true),
                    };
                    columns.insert(name, col.clone());
                    changed = true;
                    continue;
                }
            }
            match self.policy {
                Policy::Block if strict => return Err(self.drift(what)),
                Policy::AllowColumns if strict && !widening => {
                    return Err(self.drift(format!("{what} (not a widening)")));
                }
                Policy::IgnoreNew if strict => {
                    let (c, rows) = cast_down(col, &cur, f.name(), self.infinity);
                    let hold_cells = self
                        .rules
                        .as_ref()
                        .is_some_and(|r| r.cells && !r.unholdable.contains(&name));
                    for (row, reason) in rows {
                        match hold_cells {
                            true => cells.push(HeldCell {
                                row,
                                column: name.clone(),
                                reason,
                                raw: coerce::cell_text(col, row).unwrap_or_default(),
                            }),
                            false => {
                                failed.entry(row).or_insert(reason);
                            }
                        }
                    }
                    columns.insert(name, c);
                    continue;
                }
                _ => {}
            }
            if widening {
                self.fields[i] = widened(&cur, f);
                columns.insert(name, col.clone());
            } else {
                if let Some(left) = self.rules.as_mut().and_then(|r| r.columns_left.as_mut()) {
                    *left = left.saturating_sub(1);
                }
                let k = self.versions.get(f.name()).map_or(2, |k| k + 1);
                self.versions.insert(f.name().clone(), k);
                let name = self.current(f.name());
                versioned.push(format!(
                    "column {:?} changed from {} to {}, so its values now go to {name:?}",
                    f.name(),
                    logical_type(&cur),
                    logical_type(f)
                ));
                self.add(f.as_ref().clone().with_name(&name).with_nullable(true));
                columns.insert(name, col.clone());
            }
            changed = true;
        }

        let n = batch.num_rows();
        let arrays: Vec<ArrayRef> = self
            .fields
            .iter()
            .map(|f| {
                columns
                    .remove(f.name())
                    .unwrap_or_else(|| new_null_array(f.data_type(), n))
            })
            .collect();
        let opts = RecordBatchOptions::new().with_row_count(Some(n));
        let mut out = RecordBatch::try_new_with_options(Arc::new(self.schema()), arrays, &opts)?;
        if !failed.is_empty() {
            let keep = BooleanArray::from_iter((0..n).map(|r| Some(!failed.contains_key(&r))));
            out = arrow_select::filter::filter_record_batch(&out, &keep)?;
            let mut row_of = vec![None; n];
            let mut k = 0;
            for (r, slot) in row_of.iter_mut().enumerate() {
                if !failed.contains_key(&r) {
                    *slot = Some(k);
                    k += 1;
                }
            }
            cells.retain_mut(|c| match row_of[c.row] {
                Some(r) => {
                    c.row = r;
                    true
                }
                None => false,
            });
        }
        let mut failed: Failures = failed.into_iter().collect();
        failed.sort_by_key(|(r, _)| *r);
        Ok(Conformed {
            batch: out,
            failed,
            changed,
            versioned,
            cells,
            held,
            big_ints_grew: self.big_ints.len() > big_ints,
        })
    }
}

/// `cur` widened to `to`'s type, keeping its name.
fn widened(cur: &Field, to: &Field) -> Field {
    let mut f = to.clone().with_name(cur.name()).with_nullable(true);
    if to.data_type() == &DataType::Utf8
        && to.metadata().get(EXTENSION_NAME_KEY).map(String::as_str) != Some(JSON_EXTENSION)
    {
        let mut md = f.metadata().clone();
        md.remove(EXTENSION_NAME_KEY);
        md.remove(EXTENSION_METADATA_KEY);
        f = f.with_metadata(md);
    }
    f
}

/// Whether an `int64` column has a value past ±2^53.
fn past_exact_float(col: &ArrayRef) -> bool {
    let Some(a) = col.as_primitive_opt::<Int64Type>() else {
        return false;
    };
    a.iter()
        .flatten()
        .any(|v| v.unsigned_abs() > MAX_EXACT_FLOAT_INT)
}

/// Whether a value's text is an integer that `float64` would round.
fn text_past_exact_float(s: &str) -> bool {
    s.parse::<i128>()
        .is_ok_and(|v| v.unsigned_abs() > u128::from(MAX_EXACT_FLOAT_INT))
}

/// Casts a column to a type that widens it.
pub(crate) fn cast_up(col: &ArrayRef, to: &Field) -> Result<ArrayRef, Error> {
    if col.data_type() == to.data_type() {
        return Ok(col.clone());
    }
    let col = &coerce::offset_zone(col);
    if arrow_cast::can_cast_types(col.data_type(), to.data_type()) {
        return Ok(arrow_cast::cast(col, to.data_type())?);
    }
    // Only text is left, through each value's display form.
    let fmt = ArrayFormatter::try_new(col.as_ref(), &FormatOptions::new())?;
    let strings: arrow_array::StringArray = (0..col.len())
        .map(|i| (!col.is_null(i)).then(|| fmt.value(i).to_string()))
        .collect();
    Ok(Arc::new(strings))
}

/// Casts a column to a narrower type, value by value through its display
/// form. Values that don't fit are null and listed.
fn cast_down(col: &ArrayRef, to: &Field, source: &str, inf: Infinity) -> (ArrayRef, Failures) {
    let fail_all = |why: &str| {
        let failed = (0..col.len())
            .filter(|&i| !col.is_null(i))
            .map(|i| (i, format!("type: column {source:?} {why}")))
            .collect();
        (new_null_array(to.data_type(), col.len()), failed)
    };
    let Some(t) = Target::of(to).filter(|t| *t != Target::TypedOnly) else {
        return fail_all(&format!("isn't {}", logical_type(to)));
    };
    let col = &coerce::offset_zone(col);
    let Ok(fmt) = ArrayFormatter::try_new(col.as_ref(), &FormatOptions::new()) else {
        return fail_all(&format!("isn't {}", logical_type(to)));
    };
    let mut rounded = Vec::new();
    let strings: arrow_array::StringArray = (0..col.len())
        .map(|i| {
            let s = (!col.is_null(i)).then(|| fmt.value(i).to_string())?;
            if t == Target::Float64 && text_past_exact_float(&s) {
                rounded.push((
                    i,
                    format!("type: column {source:?} doesn't fit a float64 exactly"),
                ));
                return None;
            }
            Some(s)
        })
        .collect();
    let named = to.clone().with_name(source);
    match coerce::text_column(&named, t, &(Arc::new(strings) as ArrayRef), inf) {
        Some((out, mut failed)) => {
            failed.extend(rounded);
            (out, failed)
        }
        None => fail_all(&format!("isn't {}", logical_type(to))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};
    use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};

    fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(cols).unwrap()
    }

    #[test]
    fn zoned_timestamps_widen_to_strings() {
        let mut e = Evolver::new(Policy::AllowAll, None, BTreeMap::new());
        let s: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        e.conform(&batch(vec![("x", s)])).unwrap();
        let ts = arrow_array::TimestampMicrosecondArray::from(vec![5]).with_timezone("UTC");
        let c = e
            .conform(&batch(vec![("x", Arc::new(ts) as ArrayRef)]))
            .unwrap();
        assert_eq!(
            c.batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("1970-01-01T00:00:00.000005Z")]
        );
    }

    fn names(s: &Schema) -> Vec<(String, DataType)> {
        s.fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    #[test]
    fn widening() {
        let f = |dt| Field::new("x", dt, true);
        assert!(widens(&f(DataType::Int32), &f(DataType::Int64)));
        assert!(widens(&f(DataType::Int64), &f(DataType::Float64)));
        assert!(widens(&f(DataType::Int32), &f(DataType::Decimal128(12, 2))));
        assert!(!widens(
            &f(DataType::Int32),
            &f(DataType::Decimal128(11, 2))
        ));
        assert!(widens(
            &f(DataType::Decimal128(10, 2)),
            &f(DataType::Decimal128(12, 4))
        ));
        assert!(!widens(
            &f(DataType::Decimal128(10, 2)),
            &f(DataType::Decimal128(12, 1))
        ));
        assert!(widens(&f(DataType::Boolean), &f(DataType::Utf8)));
        assert!(!widens(&f(DataType::Int64), &f(DataType::Int32)));
        assert!(!widens(&f(DataType::Boolean), &f(DataType::Int64)));
        let json = f(DataType::Utf8).with_metadata(HashMap::from([(
            EXTENSION_NAME_KEY.to_owned(),
            JSON_EXTENSION.to_owned(),
        )]));
        assert!(widens(&json, &f(DataType::Utf8)));
        assert!(!widens(&f(DataType::Utf8), &json));
    }

    fn ints(v: &[i64]) -> RecordBatch {
        batch(vec![(
            "x",
            Arc::new(Int64Array::from(v.to_vec())) as ArrayRef,
        )])
    }

    fn floats(v: &[f64]) -> RecordBatch {
        batch(vec![(
            "x",
            Arc::new(Float64Array::from(v.to_vec())) as ArrayRef,
        )])
    }

    const BIG: i64 = (1 << 53) + 1;

    fn strings(b: &RecordBatch) -> Vec<Option<&str>> {
        b.column(0).as_string::<i32>().iter().collect()
    }

    #[test]
    fn a_column_past_2_53_becomes_a_string_not_a_float() {
        let mut e = Evolver::new(Policy::AllowAll, None, BTreeMap::new());
        e.conform(&ints(&[BIG])).unwrap();
        let c = e.conform(&floats(&[1.5])).unwrap();
        assert!(c.changed);
        assert_eq!(names(&e.schema())[0].1, DataType::Utf8);
        assert_eq!(strings(&c.batch), vec![Some("1.5")]);
        let c = e.conform(&ints(&[BIG])).unwrap();
        assert!(!c.changed);
        assert_eq!(strings(&c.batch), vec![Some("9007199254740993")]);
    }

    #[test]
    fn big_integers_into_an_established_float_become_strings() {
        let mut e = Evolver::new(Policy::AllowAll, None, BTreeMap::new());
        e.conform(&floats(&[1.5])).unwrap();
        let c = e.conform(&ints(&[BIG, 2])).unwrap();
        assert!(c.changed);
        assert_eq!(names(&e.schema())[0].1, DataType::Utf8);
        assert_eq!(strings(&c.batch), vec![Some("9007199254740993"), Some("2")]);
    }

    #[test]
    fn integers_within_2_53_still_widen_to_float() {
        let mut e = Evolver::new(Policy::AllowAll, None, BTreeMap::new());
        e.conform(&ints(&[1 << 53])).unwrap();
        let c = e.conform(&floats(&[1.5])).unwrap();
        assert!(c.changed);
        assert_eq!(names(&e.schema())[0].1, DataType::Float64);
    }

    #[test]
    fn allow_columns_takes_a_big_integer_column_going_to_string() {
        let mut e = Evolver::new(Policy::AllowColumns, None, BTreeMap::new());
        e.conform(&ints(&[BIG])).unwrap();
        e.establish();
        let c = e.conform(&floats(&[1.5])).unwrap();
        assert!(c.changed);
        assert_eq!(names(&e.schema())[0].1, DataType::Utf8);
    }

    #[test]
    fn ignore_new_quarantines_a_big_integer_a_float_column_would_round() {
        let mut e = Evolver::new(Policy::IgnoreNew, None, BTreeMap::new());
        e.conform(&floats(&[1.5])).unwrap();
        e.establish();
        let c = e.conform(&ints(&[BIG, 2])).unwrap();
        assert!(!c.changed);
        assert_eq!(c.failed.len(), 1);
        assert_eq!(c.failed[0].0, 0);
        assert_eq!(names(&e.schema())[0].1, DataType::Float64);
    }

    #[test]
    fn batches_conform_and_types_only_widen() {
        let mut e = Evolver::new(Policy::AllowAll, None, BTreeMap::new());
        let a = batch(vec![("x", Arc::new(Int64Array::from(vec![1])) as ArrayRef)]);
        let c = e.conform(&a).unwrap();
        assert!(c.changed);
        e.establish();

        // Narrower: cast up. New column: added. Missing: null.
        let b = batch(vec![
            ("y", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
            ("x", Arc::new(Int32Array::from(vec![2]))),
        ]);
        let c = e.conform(&b).unwrap();
        assert!(c.changed);
        assert_eq!(
            names(&c.batch.schema()),
            vec![("x".into(), DataType::Int64), ("y".into(), DataType::Utf8)]
        );
        assert_eq!(c.batch.column(0).as_primitive::<Int64Type>().values()[0], 2);

        // Wider: the schema widens.
        let w = batch(vec![(
            "x",
            Arc::new(Float64Array::from(vec![2.5])) as ArrayRef,
        )]);
        let c = e.conform(&w).unwrap();
        assert!(c.changed);
        assert_eq!(c.batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(c.batch.column(1).null_count(), 1);
        let c = e.conform(&a).unwrap();
        assert!(!c.changed);
        assert_eq!(
            c.batch.column(0).as_primitive::<Float64Type>().values()[0],
            1.0
        );
    }

    #[test]
    fn incompatible_types_get_a_new_version() {
        let mut e = Evolver::new(Policy::AllowAll, None, BTreeMap::new());
        e.conform(&batch(vec![(
            "x",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )]))
        .unwrap();
        e.establish();
        let b = batch(vec![(
            "x",
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        )]);
        let c = e.conform(&b).unwrap();
        assert!(c.changed);
        assert_eq!(
            names(&c.batch.schema()),
            vec![
                ("x".into(), DataType::Int64),
                ("x__v2".into(), DataType::Boolean)
            ]
        );
        assert_eq!(c.batch.column(0).null_count(), 1);
        assert_eq!(e.versions().get("x"), Some(&2));
        assert_eq!(names(&e.live()), vec![("x".into(), DataType::Boolean)]);

        // Restored, the version holds, and the next change is v3.
        let mut r = Evolver::new(Policy::AllowAll, Some(&e.schema()), e.versions().clone());
        let c = r.conform(&b).unwrap();
        assert!(!c.changed);
        let s = batch(vec![("x", Arc::new(Int32Array::from(vec![1])) as ArrayRef)]);
        r.conform(&s).unwrap();
        assert_eq!(r.versions().get("x"), Some(&3));
        assert_eq!(names(&r.live()), vec![("x".into(), DataType::Int32)]);
    }

    #[test]
    fn policies() {
        let start = |p| {
            let mut e = Evolver::new(p, None, BTreeMap::new());
            e.conform(&batch(vec![(
                "x",
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            )]))
            .unwrap();
            e.establish();
            e
        };
        let new_col = batch(vec![
            ("x", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            ("y", Arc::new(Int32Array::from(vec![1]))),
        ]);
        let wider = batch(vec![(
            "x",
            Arc::new(Float64Array::from(vec![1.0, 1.5])) as ArrayRef,
        )]);
        let other = batch(vec![(
            "x",
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        )]);

        let mut e = start(Policy::Block);
        assert!(matches!(e.conform(&new_col), Err(Error::Drift(_))));
        assert!(matches!(e.conform(&wider), Err(Error::Drift(_))));

        let mut e = start(Policy::AllowColumns);
        assert!(e.conform(&new_col).unwrap().changed);
        assert!(e.conform(&wider).unwrap().changed);
        assert!(matches!(e.conform(&other), Err(Error::Drift(_))));

        let mut e = start(Policy::IgnoreNew);
        let c = e.conform(&new_col).unwrap();
        assert!(!c.changed);
        assert_eq!(c.batch.num_columns(), 1);
        let c = e.conform(&wider).unwrap();
        assert!(!c.changed);
        assert_eq!(c.batch.num_rows(), 1);
        assert_eq!(c.failed.len(), 1);
        assert_eq!(c.failed[0].0, 1);
        assert!(
            c.failed[0].1.starts_with("type: column \"x\""),
            "{}",
            c.failed[0].1
        );
        let c = e.conform(&other).unwrap();
        assert_eq!(c.batch.num_rows(), 0);

        // Before a schema is established, every policy takes changes.
        let mut e = Evolver::new(Policy::Block, None, BTreeMap::new());
        e.conform(&new_col).unwrap();
        assert!(e.conform(&wider).unwrap().changed);
    }
}
