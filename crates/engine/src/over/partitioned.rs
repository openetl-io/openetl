//! What `OVER` and dedupe share: the statement's input partitioned by
//! `PARTITION BY` and ordered by event time, its rows written in state
//! under the schema they came with, which state keeps beside them, and the
//! watermark the operator moves.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use openetl_normalize::{Cause, Code, Control, Typed, key_groups};
use openetl_proto::connector::v1::ErrorCode;
use openetl_sql::{GroupKey, Step};
use openetl_state::{DueRow, KeyRef, OperatorId, StateBackend};

use super::row;
use crate::error::JobError;
use crate::window::codec::{self, Value};
use crate::window::{Out, steps};

/// Rows taken from state at once.
pub(crate) const TAKE: usize = 1024;
/// Rows an advance gives before it lets its caller take them.
pub(crate) const RELEASE: usize = 8192;

/// The key in state of the schema rows of hash `fp` are written under.
const SCHEMA: u8 = 2;

/// A batch of the statement's input, as the steps before the operator gave
/// it, with what each row is to the operator.
pub(crate) struct Taken {
    pub rows: RecordBatch,
    /// Each row's record ID.
    pub ids: Vec<[u8; 16]>,
    /// Each row's key group and key: `PARTITION BY`'s values.
    pub groups: Vec<u32>,
    pub keys: Vec<Vec<u8>>,
    /// Each row's event time, in its unit; None for null.
    pub times: Vec<Option<i64>>,
    /// Each row's row of the batch the operator was given.
    pub at: Vec<usize>,
}

/// The statement's input as `OVER` or dedupe takes it, on one lane's key
/// groups.
pub(crate) struct Partitioned {
    pub id: OperatorId,
    /// The statement's place and output, for messages.
    at: String,
    output: String,
    /// The rows the operator takes, as the plan has them now.
    rows: SchemaRef,
    /// Each `PARTITION BY` column of `rows`, and its stream's name.
    keys: Vec<(usize, String)>,
    key_names: Vec<String>,
    key_groups: u32,
    time: usize,
    /// The event time's unit per microsecond: 1, or 1000 for nanoseconds.
    pub per_micro: i64,
    /// In microseconds; `i64::MIN` until it advances.
    pub watermark: i64,
    /// The hash of the schema it writes rows under, and the schema.
    schema: (u64, Bytes),
    /// The schemas of rows it has read or written, by their hashes.
    schemas: HashMap<u64, SchemaRef>,
    /// The key groups whose state holds the schema it writes rows under.
    registered: HashSet<u32>,
    /// Rows taken from state that the next take may add peers to.
    held: Vec<DueRow>,
}

impl Partitioned {
    /// `id` is the statement's operator's, `at` where it is, `rows` what
    /// the operator takes, `time` the column of its event time and
    /// `key_groups` the pipeline's.
    pub fn new(
        id: &str,
        at: openetl_sql::Position,
        output: &str,
        rows: &SchemaRef,
        keys: &[GroupKey],
        time: usize,
        key_groups: u32,
    ) -> Result<Self, JobError> {
        let per_micro = match rows.field(time).data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => 1000,
            _ => 1,
        };
        let (fp, ipc) = row::fingerprint(rows).map_err(JobError::internal)?;
        Ok(Self {
            id: crate::window::operator(id),
            at: at.to_string(),
            output: output.to_owned(),
            rows: rows.clone(),
            keys: keys.iter().map(|k| (k.column, k.stream.clone())).collect(),
            key_names: keys
                .iter()
                .map(|k| rows.field(k.column).name().clone())
                .collect(),
            key_groups,
            time,
            per_micro,
            watermark: i64::MIN,
            schema: (fp, Bytes::from(ipc)),
            schemas: HashMap::from([(fp, rows.clone())]),
            registered: HashSet::new(),
            held: Vec::new(),
        })
    }

    /// Takes rows of `op` at or before `upto` from state, about [`TAKE`]
    /// at once: each key's in order of time, then of record ID and values,
    /// a key's rows of one time, its peers, together. Says whether more may
    /// be due.
    pub async fn take_due<S: StateBackend>(
        &mut self,
        state: &mut S,
        op: OperatorId,
        upto: i64,
    ) -> Result<(Vec<DueRow>, bool), JobError> {
        let taken = state
            .take_rows(op, upto, TAKE)
            .await
            .map_err(|e| super::state_error(&e))?;
        let full = taken.len() == TAKE;
        let prior = self.held.len();
        let mut rows = std::mem::take(&mut self.held);
        rows.extend(taken);
        // The next take may hold more of the last key's rows at its time,
        // its peers, which leave together. The rows held are one key's
        // peers already, so only the rows just taken can end them.
        if full && let Some(last) = rows.last() {
            let (g, t, k) = (last.group, last.time, last.key.clone());
            let differs = |r: &DueRow| r.group != g || r.time != t || r.key != k;
            let at = match rows[prior..].iter().rposition(differs) {
                Some(i) => prior + i + 1,
                None if prior > 0 && differs(&rows[prior - 1]) => prior,
                None => 0,
            };
            self.held = rows.split_off(at);
        }
        rows.sort_by(|a, b| {
            (a.group, &a.key, a.time, row::tie(&a.row)).cmp(&(
                b.group,
                &b.key,
                b.time,
                row::tie(&b.row),
            ))
        });
        Ok((rows, full))
    }

    pub fn reason(&self, why: &str) -> String {
        format!("transform.sql {}, writing {}: {why}", self.at, self.output)
    }

    pub fn corrupt(&self, why: &str) -> JobError {
        JobError::new(
            ErrorCode::CheckpointCorrupt,
            self.reason(&format!("its state: {why}")),
        )
    }

    /// Its watermark in microseconds, if it has advanced.
    pub fn watermark_micros(&self) -> Option<i64> {
        (self.watermark != i64::MIN).then_some(self.watermark)
    }

    /// Moves the watermark to `t`, in microseconds; whether it moved.
    pub fn advance(&mut self, t: i64) -> bool {
        let moved = t > self.watermark;
        self.watermark = self.watermark.max(t);
        moved
    }

    /// The latest event time, in its unit, the watermark has passed: the
    /// watermark less one, or every time at the end of input.
    pub fn left(&self) -> i64 {
        match self.watermark {
            i64::MIN => i64::MIN,
            i64::MAX => i64::MAX,
            w => w.saturating_mul(self.per_micro).saturating_sub(1),
        }
    }

    /// Whether a row at `t` in key group `g` is late: the watermark has
    /// passed it, or the rows of its time there have left, as a group a
    /// restore brought at a later horizon of `marks` had.
    pub fn late<S: StateBackend>(&self, state: &S, marks: OperatorId, g: u32, t: i64) -> bool {
        let h = state.horizon(marks, g);
        t <= self.left() || (h > i64::MIN && t <= h)
    }

    /// A batch of the statement's input, the columns it reads, with each
    /// row's record ID, through `before`, the steps before the operator:
    /// the rows they give, and the rows they reject in `Out`.
    pub fn take(
        &self,
        before: &[Step],
        input: &RecordBatch,
        ids: &FixedSizeBinaryArray,
    ) -> Result<(Taken, Out), JobError> {
        let (ran, mut out) = steps::before(before, input, |why| self.reason(why))?;
        let rows = ran.batch;
        let ids = super::ids_of(ids, &ran.rows, super::unnests(before))
            .map_err(|e| JobError::internal(self.reason(&e)))?;
        let ids = ids
            .iter()
            .enumerate()
            .map(|(r, id)| {
                id.and_then(|id| <[u8; 16]>::try_from(id).ok())
                    .ok_or_else(|| {
                        JobError::internal(self.reason(&format!("row {r} has no record ID")))
                    })
            })
            .collect::<Result<_, _>>()?;
        let (groups, keys) = self.keys(&rows, input)?;
        let times = rows.column(self.time);
        let times = (0..rows.num_rows())
            .map(|r| match Value::read(times.as_ref(), r) {
                Some(Value::Int(t)) => Some(t),
                _ => None,
            })
            .collect();
        out.rejected.sort_by_key(|(r, ..)| *r);
        Ok((
            Taken {
                rows,
                ids,
                groups,
                keys,
                times,
                at: ran.rows,
            },
            out,
        ))
    }

    /// Row `r`'s event time, or None, and it rejected in `out`: null, or
    /// late where `marks` says.
    pub fn placed<S: StateBackend>(
        &self,
        state: &S,
        marks: OperatorId,
        taken: &Taken,
        r: usize,
        out: &mut Out,
    ) -> Option<i64> {
        let Some(t) = taken.times[r] else {
            out.rejected.push((
                taken.at[r],
                self.reason("its event time is null"),
                Cause::new(Code::EventTime),
            ));
            return None;
        };
        if self.late(state, marks, taken.groups[r], t) {
            out.rejected.push((
                taken.at[r],
                self.reason("late: the watermark passed its event time before it came"),
                Cause::new(Code::Late),
            ));
            return None;
        }
        Some(t)
    }

    /// Row `r` of `taken`, written as state keeps it. The first row of a key
    /// group writes the schema there too.
    pub fn encode<S: StateBackend>(
        &mut self,
        state: &mut S,
        taken: &Taken,
        r: usize,
    ) -> Result<Vec<u8>, JobError> {
        let g = taken.groups[r];
        if self.registered.insert(g) {
            let (fp, ipc) = &self.schema;
            state.put(self.id, KeyRef::new(g, &schema_key(*fp)), ipc.clone());
        }
        let mut buf = Vec::new();
        row::encode(&taken.rows, Some(&taken.ids[r]), r, self.schema.0, &mut buf)
            .map_err(|e| JobError::internal(self.reason(&e)))?;
        Ok(buf)
    }

    /// Each row's key group and key: `PARTITION BY`'s values, equal JSON
    /// one key whatever its spaces or its objects' order, in the key group
    /// the exchange before the operator gives it.
    fn keys(
        &self,
        rows: &RecordBatch,
        input: &RecordBatch,
    ) -> Result<(Vec<u32>, Vec<Vec<u8>>), JobError> {
        let n = rows.num_rows();
        let columns: Vec<ArrayRef> = self
            .keys
            .iter()
            .map(|(column, stream)| {
                let c = rows.column(*column).clone();
                let json = crate::window::is_json(rows.schema().field(*column))
                    || input
                        .schema()
                        .field_with_name(stream)
                        .is_ok_and(crate::window::is_json);
                match json {
                    true => crate::window::canonical_json(&c),
                    false => c,
                }
            })
            .collect();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(
                self.keys
                    .iter()
                    .map(|(column, _)| rows.schema().field(*column).clone())
                    .collect::<Vec<_>>(),
            )),
            columns.clone(),
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let groups = key_groups(&batch, &self.key_names, self.key_groups)
            .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let keys = (0..n)
            .map(|r| {
                let mut k = Vec::new();
                for c in &columns {
                    codec::encode(Value::read(c.as_ref(), r).as_ref(), &mut k);
                }
                k
            })
            .collect();
        Ok((groups, keys))
    }

    /// Rows state kept, each with its key group, in order, as the plan's
    /// input to the operator, and their record IDs: each read under the
    /// schema it was written with, which state keeps beside it.
    pub async fn decode<S: StateBackend>(
        &mut self,
        state: &mut S,
        kept: &[(u32, &[u8])],
    ) -> Result<(RecordBatch, Vec<[u8; 16]>), JobError> {
        let mut by: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        for (i, (_, r)) in kept.iter().enumerate() {
            let fp = row::schema_of(r).map_err(|e| self.corrupt(&e))?;
            by.entry(fp).or_default().push(i);
        }
        let mut parts = Vec::with_capacity(by.len());
        let mut ids = vec![[0u8; 16]; kept.len()];
        let mut order = vec![0u32; kept.len()];
        let mut at = 0u32;
        for (fp, rows) in &by {
            let schema = self.schema_named(state, *fp, kept[rows[0]].0).await?;
            let bytes: Vec<&[u8]> = rows.iter().map(|&i| kept[i].1).collect();
            let (batch, got) = row::decode(&bytes, &schema).map_err(|e| self.corrupt(&e))?;
            let batch = row::conform(&batch, &self.rows).map_err(|(column, why)| {
                JobError::new(
                    ErrorCode::SchemaDrift,
                    self.reason(&format!(
                        "rows kept in state can't be read as the statement reads them now: column {column} changed {why}. Reset the stream it reads to start the operator's state again: openetl reset --stream STREAM NAME"
                    )),
                )
            })?;
            for (&i, id) in rows.iter().zip(got) {
                ids[i] = id;
                order[i] = at;
                at += 1;
            }
            parts.push(batch);
        }
        if parts.is_empty() {
            return Ok((RecordBatch::new_empty(self.rows.clone()), ids));
        }
        let all = match parts.len() {
            1 => parts.remove(0),
            _ => arrow_select::concat::concat_batches(&parts[0].schema(), &parts)
                .map_err(|e| JobError::internal(e.to_string()))?,
        };
        let rows = arrow_select::take::take_record_batch(&all, &UInt32Array::from(order))
            .map_err(|e| JobError::internal(e.to_string()))?;
        let rows = RecordBatch::try_new_with_options(
            Arc::new(Schema::new_with_metadata(
                self.rows.fields().clone(),
                self.rows.metadata().clone(),
            )),
            rows.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(rows.num_rows())),
        )
        .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        Ok((rows, ids))
    }

    /// The schema rows of hash `fp` are written under, from state's key
    /// group `g` where it isn't known yet.
    async fn schema_named<S: StateBackend>(
        &mut self,
        state: &mut S,
        fp: u64,
        g: u32,
    ) -> Result<SchemaRef, JobError> {
        if let Some(s) = self.schemas.get(&fp) {
            return Ok(s.clone());
        }
        let key = schema_key(fp);
        let got = state
            .get(self.id, &[KeyRef::new(g, &key)])
            .await
            .map_err(|e| super::state_error(&e))?;
        let Some(Some(ipc)) = got.into_iter().next() else {
            return Err(self.corrupt(&format!("rows of a schema {fp:016x} it doesn't hold")));
        };
        let schema = crate::ipc::decode_schema(&ipc).map_err(|e| self.corrupt(&e.to_string()))?;
        self.schemas.insert(fp, schema.clone());
        Ok(schema)
    }

    /// The output's rows of `batch`, rows `at` of `rows`, with their record
    /// IDs and their event times as their seq, and `ops` where they aren't
    /// all inserts.
    pub fn typed(
        &self,
        batch: RecordBatch,
        rows: &RecordBatch,
        at: &[usize],
        ids: impl Iterator<Item = [u8; 16]>,
        ops: Option<ArrayRef>,
    ) -> Result<Typed, JobError> {
        let ids = FixedSizeBinaryArray::try_from_iter(ids)
            .map_err(|e| JobError::internal(e.to_string()))?;
        let times = arrow_select::take::take(
            rows.column(self.time).as_ref(),
            &UInt32Array::from_iter_values(at.iter().map(|&r| r as u32)),
            None,
        )
        .map_err(|e| JobError::internal(e.to_string()))?;
        Ok(Typed {
            batch,
            ids,
            control: Control {
                op: ops,
                seq: Some(times),
                unchanged: None,
            },
            cells: Vec::new(),
            rejected: Vec::new(),
            rows: None,
        })
    }
}

fn schema_key(fp: u64) -> Vec<u8> {
    let mut b = vec![SCHEMA];
    b.extend_from_slice(&fp.to_be_bytes());
    b
}
