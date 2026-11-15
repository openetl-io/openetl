//! Where a job's stateful operators keep their state, and how the record
//! names it. Each operator's lanes write runs below a directory of the
//! job's state URL of the operator's own, a run per lane at each barrier
//! that changed something. A lane's snapshot names every run it reads, and
//! counts once the epoch's record does: a run no committed record names is
//! never read, and retention deletes it. A restore opens what the record
//! names, by each lane's own range of key groups, so state moves to any
//! number of lanes, and reads nothing until a lane needs it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use openetl_proto::connector::v1::ErrorCode;
use openetl_proto::engine::v1::{KeyGroupRange as ProtoRange, StateSnapshot};
use openetl_state::runs::{ObjectStorage, RunConfig, RunStores, runs};
use openetl_state::{Error as StateError, KeyGroupRange, Snapshot};

use crate::error::JobError;
use crate::store::Location;

/// What the block caches of a job's operators may hold together: a
/// sixteenth of the job's memory, and between these for each operator.
const CACHE_SHARE: u64 = 16;
const MIN_CACHE: usize = 1 << 20;
const MAX_CACHE: usize = 64 << 20;

/// A job's state: where its operators' runs are, and whether it writes
/// them.
pub struct JobState {
    location: Location,
    read_only: bool,
    config: RunConfig,
    /// Each operator's, which its lanes share with their block cache.
    stores: Mutex<HashMap<String, RunStores>>,
}

impl std::fmt::Debug for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobState")
            .field("base", &self.location.base)
            .field("read_only", &self.read_only)
            .finish()
    }
}

impl JobState {
    /// Opens `url` as [`Location::open`] does, for `operators` stateful
    /// operators in a job of `memory` bytes, 0 for no limit.
    pub fn open(
        url: &str,
        read_only: bool,
        operators: usize,
        memory: u64,
    ) -> Result<Self, JobError> {
        let location = Location::open(url, "state URL")?;
        let cache = match memory {
            0 => MAX_CACHE,
            m => usize::try_from(m / CACHE_SHARE / operators.max(1) as u64)
                .unwrap_or(MAX_CACHE)
                .clamp(MIN_CACHE, MAX_CACHE),
        };
        Ok(Self {
            location,
            read_only,
            config: RunConfig {
                cache_bytes: cache,
                ..RunConfig::default()
            },
            stores: Mutex::new(HashMap::new()),
        })
    }

    /// The job's stateful operators read their restored state and write
    /// nothing.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Where `operator`'s lanes keep their runs.
    pub fn stores(&self, operator: &str) -> RunStores {
        let mut stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        stores
            .entry(operator.to_owned())
            .or_insert_with(|| {
                let prefix = self.location.prefix.clone().join(dir(operator));
                let objects = ObjectStorage::new(self.location.store.clone(), prefix);
                RunStores::new(Arc::new(objects), self.config.clone())
            })
            .clone()
    }
}

/// The directory below the state URL that holds an operator's runs: its ID,
/// where that's only letters, digits, `-` and `_`, and otherwise `x.` and
/// the ID's bytes in hex, which no such ID can be.
pub fn dir(operator: &str) -> String {
    let plain = !operator.is_empty()
        && operator
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    match plain {
        true => operator.to_owned(),
        false => {
            let hex: String = operator.bytes().map(|b| format!("{b:02x}")).collect();
            format!("x.{hex}")
        }
    }
}

/// The record's name for a lane's snapshot of `operator`, with the runs it
/// names below the state URL.
pub fn named(operator: &str, s: &Snapshot) -> Result<StateSnapshot, JobError> {
    let dir = dir(operator);
    let names = runs(s).map_err(|e| state_error(&format!("operator {operator}'s state"), &e))?;
    Ok(StateSnapshot {
        operator: operator.to_owned(),
        key_groups: Some(ProtoRange {
            first: s.range.first,
            last: s.range.last,
        }),
        layout: s.layout,
        data: s.data.to_vec(),
        runs: names
            .into_iter()
            .map(|r| format!("{dir}/{}", r.name))
            .collect(),
    })
}

/// A snapshot the record names, and its operator.
pub fn snapshot(s: &StateSnapshot) -> Result<(String, Snapshot), JobError> {
    let Some(r) = &s.key_groups else {
        return Err(JobError::new(
            ErrorCode::CheckpointCorrupt,
            format!(
                "the checkpoint names state of operator {:?} without its key groups",
                s.operator
            ),
        ));
    };
    Ok((
        s.operator.clone(),
        Snapshot {
            range: KeyGroupRange::new(r.first, r.last),
            layout: s.layout,
            data: Bytes::copy_from_slice(&s.data),
        },
    ))
}

/// A state error as the job's, `what` saying whose: storage that failed,
/// which may not again; state the checkpoint names that can't be read as
/// it says; or the engine's own mistake.
pub fn state_error(what: &str, e: &StateError) -> JobError {
    let code = match e {
        StateError::Store(_) => ErrorCode::Transient,
        StateError::Snapshot(_) | StateError::Corrupt(_) => ErrorCode::CheckpointCorrupt,
        StateError::Invalid(_) => ErrorCode::Internal,
    };
    JobError::new(code, format!("{what}: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use openetl_state::runs::{RunRef, encode};

    #[test]
    fn a_snapshot_reads_back_as_named_with_its_runs() {
        let s = Snapshot {
            range: KeyGroupRange::new(64, 127),
            layout: openetl_state::LAYOUT,
            data: encode(&[RunRef {
                name: "g2/e9/kg64-127.run".into(),
                format: openetl_state::runs::FORMAT,
                size: 10,
                sections: Vec::new(),
            }]),
        };
        let named = named("window-1", &s).unwrap();
        assert_eq!(named.runs, ["window-1/g2/e9/kg64-127.run"]);
        assert_eq!(snapshot(&named).unwrap(), ("window-1".into(), s));
        let bad = StateSnapshot {
            key_groups: None,
            ..named
        };
        assert_eq!(
            snapshot(&bad).unwrap_err().code,
            ErrorCode::CheckpointCorrupt
        );
    }

    #[test]
    fn an_operators_directory_is_its_id_or_its_bytes() {
        assert_eq!(dir("window-0123abcd_x"), "window-0123abcd_x");
        assert_eq!(dir("a/b"), "x.612f62");
        assert_eq!(dir("x.61"), "x.782e3631");
        assert_eq!(dir(""), "x.");
    }
}
