//! Where a job's stateful operators keep their state, and how the record
//! names it. The store is the process's memory for now: what it holds
//! outlives an epoch and a lane, but not the engine, so a restore finds a
//! record's state only in the engine that wrote it. A store on object
//! storage takes its place behind the same [`Stores`]. Each operator of a
//! pipeline has a store of its own, and each lane a layer of it for its key
//! groups.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use bytes::Bytes;
use openetl_proto::engine::v1::{KeyGroupRange as ProtoRange, StateSnapshot};
use openetl_state::{KeyGroupRange, MemoryBucket, Snapshot, Stores};

use crate::error::JobError;

static BUCKETS: LazyLock<Mutex<HashMap<(String, String), MemoryBucket>>> =
    LazyLock::new(Default::default);

/// Where a pipeline's operator keeps its state.
pub fn stores(pipeline: &str, operator: &str) -> Arc<dyn Stores> {
    let mut buckets = BUCKETS.lock().unwrap_or_else(PoisonError::into_inner);
    let bucket = buckets
        .entry((pipeline.to_owned(), operator.to_owned()))
        .or_default();
    Arc::new(bucket.clone())
}

/// A memory store's snapshot's URL: `memory:LAYOUT/NAME`.
const MEMORY: &str = "memory:";

/// The record's name for an operator's snapshot.
pub fn named(operator: &str, s: &Snapshot) -> StateSnapshot {
    StateSnapshot {
        operator: operator.to_owned(),
        url: format!("{MEMORY}{}/{}", s.layout, String::from_utf8_lossy(&s.data)),
        key_groups: Some(ProtoRange {
            first: s.range.first,
            last: s.range.last,
        }),
    }
}

/// A snapshot the record names, and its operator.
pub fn snapshot(s: &StateSnapshot, key_groups: u32) -> Result<(String, Snapshot), JobError> {
    let bad = |why: &str| {
        JobError::config(format!(
            "operator {}'s state at {}: {why}",
            s.operator, s.url
        ))
    };
    let Some((layout, name)) = s
        .url
        .strip_prefix(MEMORY)
        .and_then(|rest| rest.split_once('/'))
    else {
        return Err(bad(
            "this engine keeps state in its memory, and reads no other",
        ));
    };
    let layout = layout
        .parse()
        .map_err(|_| bad("its layout isn't a number"))?;
    let range = match &s.key_groups {
        Some(r) => KeyGroupRange::new(r.first, r.last),
        None => KeyGroupRange::new(0, key_groups.saturating_sub(1)),
    };
    Ok((
        s.operator.clone(),
        Snapshot {
            range,
            layout,
            data: Bytes::copy_from_slice(name.as_bytes()),
        },
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_reads_back_as_named() {
        let s = Snapshot {
            range: KeyGroupRange::new(64, 127),
            layout: openetl_state::LAYOUT,
            data: Bytes::from_static(b"g2/e9/kg64-127"),
        };
        let named = named("window-1", &s);
        assert_eq!(named.url, "memory:1/g2/e9/kg64-127");
        assert_eq!(snapshot(&named, 256).unwrap(), ("window-1".into(), s));
        let other = StateSnapshot {
            url: "s3://bucket/state".into(),
            ..named
        };
        assert!(snapshot(&other, 256).is_err());
    }
}
