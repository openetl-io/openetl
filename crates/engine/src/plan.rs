use std::collections::{HashMap, HashSet};

use arrow_schema::SchemaRef;
use openetl_normalize::hold::{ColumnHold, HoldConfig, Limits, Show, Why};
use openetl_normalize::naming::{Namer, Naming, RESERVED_TABLES, TABLE_KEY};
use openetl_normalize::{Identity, Infinity, Policy, TypeOptions};
use openetl_proto::engine::v1::{
    EdgeDirection, Held, HeldMode, Routing, SchemaPolicy, StartJobRequest, StreamPlan, sink_plan,
};

use crate::error::JobError;
use crate::ipc;
use crate::quarantine::Quarantine;

/// A validated StartJobRequest.
#[derive(Debug)]
pub struct Plan {
    pub generation: u64,
    pub first_epoch: u64,
    pub streams: Vec<String>,
    /// Each stream's destination table.
    pub tables: Vec<String>,
    pub naming: Naming,
    pub policy: Policy,
    /// Each stream's.
    pub types: Vec<TypeOptions>,
    /// Each stream's schema in the restored checkpoint.
    pub restored: Vec<Option<SchemaRef>>,
    /// Each stream's declared logical types, under source names.
    pub declared: Vec<Option<SchemaRef>>,
    pub identities: Vec<Identity>,
    /// Each stream's: whether it gets `_openetl_deleted`.
    pub soft_deletes: Vec<bool>,
    /// Each stream's: whether it's a changelog.
    pub changelog: Vec<bool>,
    pub quarantine: Option<std::sync::Arc<Quarantine>>,
    /// Batches skip shaping, for measuring the boundary.
    pub passthrough: bool,
    /// One edge per lane, in lane order.
    pub inbound: Vec<EdgePlan>,
    pub sink: Sink,
    pub writer_lanes: usize,
    pub key_groups: u32,
    /// Each stream's: rows go to the writer lane owning their key's group,
    /// not straight from their lane.
    pub routed: Vec<bool>,
    /// Each stream's holds; None where it holds nothing.
    pub holds: Vec<Option<HoldConfig>>,
    /// Each stream's replay of released holds.
    pub replays: Vec<Option<ReplayPlan>>,
    /// Where held values go; set if any stream holds.
    pub held_url: Option<String>,
    /// The job's SQL. StartJob doesn't carry it yet, so only tests set it.
    pub transform: Option<std::sync::Arc<crate::transform::Transforms>>,
}

/// Held values to patch in, a chunk after each of the stream's barriers.
#[derive(Debug, Clone, Default)]
pub struct ReplayPlan {
    /// Below the held URL.
    pub files: Vec<String>,
    /// Where to start: a file's index in files, and its row.
    pub file: usize,
    pub row: u64,
    pub still_held: u64,
    /// Destination names.
    pub columns: HashSet<String>,
    pub chunk_rows: u64,
    /// Released column holds whose values go to a new version.
    pub versioned: Vec<ColumnHold>,
}

/// Patch rows per chunk, by default.
const CHUNK_ROWS: u64 = 50_000;

fn show(h: Option<&Held>) -> Show {
    let Some(h) = h else { return Show::Null };
    let value = Some(h.value.clone()).filter(|v| !v.is_empty());
    match h.mode() {
        HeldMode::Unspecified | HeldMode::Null => Show::Null,
        HeldMode::Value => value.map_or(Show::Null, Show::Value),
        HeldMode::Last => Show::Last(value),
    }
}

fn holds(s: &StreamPlan) -> Result<(Option<HoldConfig>, Option<ReplayPlan>), JobError> {
    let Some(h) = &s.holds else {
        return Ok((None, None));
    };
    let bad = |what: String| JobError::config(format!("stream {}: holds: {what}", s.name));
    if s.primary_key.is_empty() {
        return Err(bad("only a keyed stream holds columns".into()));
    }
    let column_holds = |cs: &[openetl_proto::engine::v1::ColumnHold]| {
        let mut out = Vec::with_capacity(cs.len());
        for c in cs {
            let delivered = match c.delivered.is_empty() {
                true => None,
                false => {
                    let schema = ipc::decode_schema(&c.delivered)
                        .map_err(|e| bad(format!("column {}: {e}", c.column)))?;
                    let [f] = schema.fields().iter().collect::<Vec<_>>()[..] else {
                        return Err(bad(format!("column {}: not one field", c.column)));
                    };
                    Some(f.as_ref().clone())
                }
            };
            out.push(ColumnHold {
                column: c.column.clone(),
                delivered,
                reason: c.reason.clone(),
                why: why(c.cause.as_ref()),
                source: String::new(),
            });
        }
        Ok(out)
    };
    let holding = column_holds(&h.holding)?;
    let limits = h.limits.as_ref().map_or_else(Limits::default, |l| Limits {
        not_null: l.not_null.iter().cloned().collect(),
        unholdable: l.unholdable.iter().cloned().collect(),
        fixed: l.fixed.iter().cloned().collect(),
        columns: l.columns as usize,
        max_columns: l.max_columns as usize,
    });
    let config = HoldConfig {
        cells: h.cells,
        show: show(h.held.as_ref()),
        columns: h
            .columns
            .iter()
            .map(|(c, held)| (c.clone(), show(Some(held))))
            .collect(),
        groups: h.groups.iter().map(|g| g.columns.clone()).collect(),
        holding,
        discarded: h.discarded.iter().cloned().collect(),
        limits,
    };
    let replay = match &h.replay {
        Some(r) => Some((r, column_holds(&r.versioned)?)),
        None => None,
    };
    let replay = replay.map(|(r, versioned)| {
        let from = r.from.unwrap_or_default();
        ReplayPlan {
            files: r.files.clone(),
            file: from.file as usize,
            row: from.row,
            still_held: from.still_held,
            columns: r.columns.iter().cloned().collect(),
            chunk_rows: match r.chunk_rows {
                0 => CHUNK_ROWS,
                n => n,
            },
            versioned,
        }
    });
    Ok((Some(config), replay))
}

#[derive(Debug)]
pub enum Sink {
    /// A Go sink in the runner, fed by an edge per writer lane, in order,
    /// each carrying every stream.
    Runner {
        edges: Vec<EdgePlan>,
        /// By edge, each stream's index on it.
        out_index: Vec<Vec<u32>>,
    },
    Parquet {
        url: String,
        /// Keeps its files apart from other pipelines' under the url.
        pipeline: String,
    },
    /// Parquet staged below url for a destination connector to load, in
    /// each epoch's dir if it isn't empty.
    Staged {
        url: String,
        dir: String,
    },
    Discard,
}

#[derive(Debug)]
pub struct EdgePlan {
    pub id: String,
    /// Indexes into Plan::streams, in the edge's order.
    pub streams: Vec<usize>,
}

impl Plan {
    pub fn from_request(req: StartJobRequest) -> Result<Plan, JobError> {
        if req.generation == 0 || req.first_epoch == 0 {
            return Err(JobError::config("generation and first_epoch start at 1"));
        }
        if req.streams.is_empty() {
            return Err(JobError::config("the job has no streams"));
        }
        if let Some(s) = req.restored_snapshots.first() {
            return Err(JobError::config(format!(
                "the job has no operator {:?} to restore",
                s.operator
            )));
        }
        let (lanes, writer_lanes) = (req.lanes.max(1) as usize, req.writer_lanes.max(1) as usize);
        if writer_lanes > lanes {
            return Err(JobError::config(format!(
                "{writer_lanes} writer lanes for {lanes} lanes"
            )));
        }
        if req.key_groups != 0 && !req.key_groups.is_power_of_two() {
            return Err(JobError::config(format!(
                "{} key groups, not a power of two",
                req.key_groups
            )));
        }
        let mut index = HashMap::new();
        let mut restored = Vec::with_capacity(req.streams.len());
        let mut declared = Vec::with_capacity(req.streams.len());
        for (i, s) in req.streams.iter().enumerate() {
            if s.name.is_empty() || index.insert(s.name.as_str(), i).is_some() {
                return Err(JobError::config(format!(
                    "stream name {:?} is empty or repeated",
                    s.name
                )));
            }
            if s.routing() == Routing::KeyGroup && s.primary_key.is_empty() {
                return Err(JobError::config(format!(
                    "stream {} is routed by key group, and has no primary key",
                    s.name
                )));
            }
            declared.push(if s.arrow_schema.is_empty() {
                None
            } else {
                let schema = ipc::decode_schema(&s.arrow_schema).map_err(|e| {
                    JobError::config(format!("stream {}: declared schema: {e}", s.name))
                })?;
                openetl_normalize::check_declared(&schema)
                    .map_err(|e| JobError::config(format!("stream {}: {e}", s.name)))?;
                Some(schema)
            });
            restored.push(if s.restored_schema.is_empty() {
                None
            } else {
                Some(ipc::decode_schema(&s.restored_schema).map_err(|e| {
                    JobError::config(format!("stream {}: restored schema: {e}", s.name))
                })?)
            });
        }
        let names: Vec<String> = req.streams.iter().map(|s| s.name.clone()).collect();
        let quarantine = match req.quarantine_url.as_str() {
            "" => None,
            url => Some(std::sync::Arc::new(Quarantine::open(url)?)),
        };
        let identities = req
            .streams
            .iter()
            .map(|s| Identity {
                stream: s.name.clone(),
                key: s.primary_key.clone(),
                cursor: Some(s.cursor_field.clone()).filter(|c| !c.is_empty()),
                read_start: s.read_start_micros,
            })
            .collect();
        let soft_deletes = req.streams.iter().map(|s| s.soft_deletes).collect();
        let changelog = req.streams.iter().map(|s| s.changelog).collect();
        let naming = req.naming.map_or_else(Naming::default, |n| Naming {
            keep_source: n.keep_source_names,
            max_len: n.max_length as usize,
        });
        let policy = match req.schema_policy() {
            SchemaPolicy::Unspecified | SchemaPolicy::AllowAll => Policy::AllowAll,
            SchemaPolicy::AllowColumns => Policy::AllowColumns,
            SchemaPolicy::Block => Policy::Block,
            SchemaPolicy::IgnoreNew => Policy::IgnoreNew,
        };
        let infinity = match req.quarantine_infinity {
            true => Infinity::Quarantine,
            false => Infinity::Clamp,
        };
        let types = req
            .streams
            .iter()
            .map(|s| TypeOptions {
                infinity,
                nanoseconds: s.nanosecond_timestamps,
            })
            .collect();
        let mut namer = Namer::new(naming, &RESERVED_TABLES);
        // A table the pipeline names comes first, and replaces a restored one.
        for s in &req.streams {
            if !s.table.is_empty() && !namer.keep(&s.name, &s.table) {
                return Err(JobError::config(format!(
                    "stream {}: table {:?} is reserved, or another stream's",
                    s.name, s.table
                )));
            }
        }
        for ((name, schema), s) in names.iter().zip(&restored).zip(&req.streams) {
            if !s.table.is_empty() {
                continue;
            }
            if let Some(table) = schema.as_ref().and_then(|s| s.metadata().get(TABLE_KEY))
                && !namer.keep(name, table)
            {
                return Err(JobError::config(format!(
                    "stream {name}: its restored table {table:?} belongs to another stream"
                )));
            }
        }
        let tables: Vec<String> = names.iter().map(|n| namer.name(n)).collect();
        let sink = req
            .sink
            .and_then(|s| s.sink)
            .ok_or_else(|| JobError::config("the job has no sink"))?;
        match &sink {
            sink_plan::Sink::Runner(_) => {}
            sink_plan::Sink::Parquet(p) => {
                openetl_formats::check(&p.url, &tables)
                    .map_err(|e| JobError::config(e.to_string()))?;
            }
            sink_plan::Sink::Staged(_) | sink_plan::Sink::Discard(_) => {}
        }

        let mut ids = HashSet::new();
        let mut inbound: Vec<Option<EdgePlan>> = (0..lanes).map(|_| None).collect();
        let mut outbound: Vec<Option<EdgePlan>> = (0..writer_lanes).map(|_| None).collect();
        for e in &req.edges {
            if e.id.is_empty() || !ids.insert(e.id.as_str()) {
                return Err(JobError::config(format!(
                    "edge id {:?} is empty or repeated",
                    e.id
                )));
            }
            let mut streams = Vec::with_capacity(e.streams.len());
            for name in &e.streams {
                let &i = index.get(name.as_str()).ok_or_else(|| {
                    JobError::config(format!("edge {} names unknown stream {name:?}", e.id))
                })?;
                if streams.contains(&i) {
                    return Err(JobError::config(format!(
                        "edge {} names stream {name:?} twice",
                        e.id
                    )));
                }
                streams.push(i);
            }
            let edge = EdgePlan {
                id: e.id.clone(),
                streams,
            };
            let slots = match e.direction() {
                EdgeDirection::ToEngine => &mut inbound,
                EdgeDirection::FromEngine if !matches!(sink, sink_plan::Sink::Runner(_)) => {
                    return Err(JobError::config(format!(
                        "edge {} leaves the engine but isn't the sink's",
                        e.id
                    )));
                }
                EdgeDirection::FromEngine => &mut outbound,
                EdgeDirection::Unspecified => {
                    return Err(JobError::config(format!("edge {} has no direction", e.id)));
                }
            };
            let Some(slot) = slots.get_mut(e.lane as usize) else {
                return Err(JobError::config(format!(
                    "edge {} is on lane {}, and the job has {} of its kind",
                    e.id,
                    e.lane,
                    slots.len()
                )));
            };
            if slot.replace(edge).is_some() {
                return Err(JobError::config(format!(
                    "two edges on lane {} of the same direction",
                    e.lane
                )));
            }
        }
        let inbound = inbound
            .into_iter()
            .enumerate()
            .map(|(lane, e)| e.ok_or_else(|| JobError::config(format!("lane {lane} has no edge"))))
            .collect::<Result<Vec<_>, _>>()?;
        for (i, s) in req.streams.iter().enumerate() {
            if inbound.iter().any(|e| !e.streams.contains(&i)) {
                return Err(JobError::config(format!(
                    "stream {:?} must arrive on every lane's edge",
                    s.name
                )));
            }
        }
        let sink = match sink {
            sink_plan::Sink::Runner(_) => {
                let edges = outbound
                    .into_iter()
                    .enumerate()
                    .map(|(lane, e)| {
                        e.ok_or_else(|| {
                            JobError::config(format!("the sink has no edge on lane {lane}"))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut out_index = Vec::with_capacity(edges.len());
                for edge in &edges {
                    if edge.streams.len() != names.len() {
                        return Err(JobError::config(format!(
                            "the sink's edge {} must carry every stream",
                            edge.id
                        )));
                    }
                    let mut index = vec![0; names.len()];
                    for (pos, &i) in edge.streams.iter().enumerate() {
                        index[i] = pos as u32;
                    }
                    out_index.push(index);
                }
                Sink::Runner { edges, out_index }
            }
            sink_plan::Sink::Parquet(p) => Sink::Parquet {
                url: p.url,
                pipeline: req.pipeline.clone(),
            },
            sink_plan::Sink::Staged(s) => {
                if s.dir.contains('/') || s.dir == "." || s.dir == ".." {
                    return Err(JobError::config(format!(
                        "the staged sink's dir {:?} isn't one path segment",
                        s.dir
                    )));
                }
                Sink::Staged {
                    url: s.url,
                    dir: s.dir,
                }
            }
            sink_plan::Sink::Discard(_) => Sink::Discard,
        };

        let mut holds_ = Vec::with_capacity(req.streams.len());
        let mut replays = Vec::with_capacity(req.streams.len());
        for st in &req.streams {
            let (h, r) = holds(st)?;
            holds_.push(h);
            replays.push(r);
        }
        let held_url = Some(req.held_url.clone()).filter(|u| !u.is_empty());
        if holds_.iter().any(Option::is_some) {
            if held_url.is_none() {
                return Err(JobError::config(
                    "streams hold columns, and the job has no held_url",
                ));
            }
            if !matches!(sink, Sink::Runner { .. }) {
                return Err(JobError::config("only a runner sink takes holds"));
            }
        }

        Ok(Plan {
            generation: req.generation,
            first_epoch: req.first_epoch,
            streams: names,
            tables,
            naming,
            policy,
            types,
            restored,
            declared,
            identities,
            soft_deletes,
            changelog,
            quarantine,
            passthrough: req.passthrough,
            inbound,
            sink,
            writer_lanes,
            key_groups: match req.key_groups {
                0 => 256,
                n => n,
            },
            routed: req
                .streams
                .iter()
                .map(|s| s.routing() == Routing::KeyGroup)
                .collect(),
            holds: holds_,
            replays,
            held_url,
            transform: None,
        })
    }
}

fn why(c: Option<&openetl_proto::engine::v1::HoldCause>) -> Why {
    use openetl_proto::engine::v1::hold_cause::Kind;
    let Some(c) = c else { return Why::Unknown };
    match c.kind() {
        Kind::Policy => Why::Policy(c.policy.clone()),
        Kind::Refused => Why::Refused,
        Kind::Fixed => Why::Fixed,
        Kind::ColumnLimit => Why::Full,
        Kind::Group => Why::Group(c.with.clone()),
        Kind::Unspecified => Why::Unknown,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(crate) mod tests {
    use super::*;
    use openetl_proto::engine::v1::{
        DiscardSink, Edge, ParquetSink, RunnerSink, SinkPlan, StagedSink, StateSnapshot, StreamPlan,
    };

    fn edge(id: &str, dir: EdgeDirection, streams: &[&str]) -> Edge {
        Edge {
            id: id.into(),
            direction: dir as i32,
            streams: streams.iter().map(|s| s.to_string()).collect(),
            lane: 0,
        }
    }

    pub(crate) fn request(streams: &[&str]) -> StartJobRequest {
        StartJobRequest {
            pipeline: "p".into(),
            job: "j".into(),
            generation: 1,
            first_epoch: 1,
            streams: streams
                .iter()
                .map(|s| StreamPlan {
                    name: s.to_string(),
                    ..Default::default()
                })
                .collect(),
            edges: vec![
                edge("in", EdgeDirection::ToEngine, streams),
                edge(
                    "out",
                    EdgeDirection::FromEngine,
                    &streams.iter().rev().copied().collect::<Vec<_>>(),
                ),
            ],
            sink: Some(SinkPlan {
                sink: Some(sink_plan::Sink::Runner(RunnerSink {})),
            }),
            limits: None,
            restored_snapshots: Vec::new(),
            naming: None,
            quarantine_url: String::new(),
            schema_policy: 0,
            quarantine_infinity: false,
            passthrough: false,
            held_url: String::new(),
            lanes: 0,
            writer_lanes: 0,
            key_groups: 0,
        }
    }

    pub(crate) fn parquet_request(streams: &[&str], url: &str) -> StartJobRequest {
        let mut req = request(streams);
        req.edges.pop();
        req.sink = Some(SinkPlan {
            sink: Some(sink_plan::Sink::Parquet(ParquetSink { url: url.into() })),
        });
        req
    }

    #[test]
    fn holds_need_a_key_a_held_url_and_a_runner_sink() {
        let holding = |req: &mut StartJobRequest| {
            req.streams[0].primary_key = vec!["id".into()];
            req.streams[0].holds = Some(openetl_proto::engine::v1::HoldPlan {
                cells: true,
                ..Default::default()
            });
            req.held_url = "file:///tmp/held".into();
        };
        let mut req = request(&["a", "b"]);
        holding(&mut req);
        let p = Plan::from_request(req).unwrap();
        assert!(p.holds[0].as_ref().is_some_and(|h| h.cells) && p.holds[1].is_none());

        let mut req = request(&["a"]);
        holding(&mut req);
        req.held_url.clear();
        assert!(
            Plan::from_request(req)
                .unwrap_err()
                .message
                .contains("held_url")
        );

        let mut req = request(&["a"]);
        holding(&mut req);
        req.streams[0].primary_key.clear();
        assert!(
            Plan::from_request(req)
                .unwrap_err()
                .message
                .contains("keyed")
        );

        let mut req = parquet_request(&["a"], "file:///tmp/p");
        holding(&mut req);
        assert!(
            Plan::from_request(req)
                .unwrap_err()
                .message
                .contains("runner sink")
        );
    }

    #[test]
    fn maps_streams_to_edges() {
        let p = Plan::from_request(request(&["a", "b", "c"])).unwrap();
        assert_eq!(p.inbound.len(), 1);
        assert_eq!(p.inbound[0].streams, vec![0, 1, 2]);
        let Sink::Runner { edges, out_index } = p.sink else {
            panic!("{:?}", p.sink)
        };
        assert_eq!(edges[0].streams, vec![2, 1, 0]);
        assert_eq!(out_index, vec![vec![2, 1, 0]]);
    }

    pub(crate) fn lanes_request(streams: &[&str], lanes: u32, writers: u32) -> StartJobRequest {
        let mut req = request(streams);
        req.lanes = lanes;
        req.writer_lanes = writers;
        req.edges.clear();
        for l in 0..lanes {
            let mut e = edge(&format!("in-{l}"), EdgeDirection::ToEngine, streams);
            e.lane = l;
            req.edges.push(e);
        }
        for w in 0..writers {
            let mut e = edge(&format!("out-{w}"), EdgeDirection::FromEngine, streams);
            e.lane = w;
            req.edges.push(e);
        }
        req
    }

    #[test]
    fn lanes_take_an_edge_each_in_lane_order() {
        let mut req = lanes_request(&["a", "b"], 3, 2);
        req.edges.reverse();
        let p = Plan::from_request(req).unwrap();
        let ids: Vec<_> = p.inbound.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["in-0", "in-1", "in-2"]);
        let Sink::Runner { edges, .. } = &p.sink else {
            panic!("{:?}", p.sink)
        };
        let ids: Vec<_> = edges.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["out-0", "out-1"]);
        assert_eq!(p.writer_lanes, 2);
        assert_eq!(p.key_groups, 256);
    }

    #[test]
    fn checks_lanes_and_their_edges() {
        let refused = |req: StartJobRequest, what: &str| {
            let e = Plan::from_request(req).unwrap_err().message;
            assert!(e.contains(what), "{e}");
        };
        refused(lanes_request(&["a"], 1, 2), "2 writer lanes for 1 lanes");
        let mut req = lanes_request(&["a"], 2, 1);
        req.edges.remove(1);
        refused(req, "lane 1 has no edge");
        let mut req = lanes_request(&["a"], 2, 2);
        req.edges.pop();
        refused(req, "the sink has no edge on lane 1");
        let mut req = lanes_request(&["a"], 2, 1);
        req.edges[1].lane = 0;
        refused(req, "two edges on lane 0");
        let mut req = lanes_request(&["a"], 2, 1);
        req.edges[1].lane = 5;
        refused(req, "on lane 5");
        let mut req = lanes_request(&["a", "b"], 2, 1);
        req.edges[1].streams.pop();
        refused(req, "must arrive on every lane's edge");
        let mut req = lanes_request(&["a"], 2, 2);
        req.key_groups = 100;
        refused(req, "not a power of two");
    }

    #[test]
    fn type_options_follow_the_job_and_each_stream() {
        let mut req = request(&["a", "b"]);
        req.quarantine_infinity = true;
        req.streams[1].nanosecond_timestamps = true;
        let p = Plan::from_request(req).unwrap();
        let q = Infinity::Quarantine;
        assert_eq!(
            p.types,
            vec![
                TypeOptions {
                    infinity: q,
                    nanoseconds: false
                },
                TypeOptions {
                    infinity: q,
                    nanoseconds: true
                },
            ]
        );
    }

    #[test]
    fn soft_deletes_are_per_stream() {
        let mut req = request(&["a", "b"]);
        req.streams[1].soft_deletes = true;
        let p = Plan::from_request(req).unwrap();
        assert_eq!(p.soft_deletes, vec![false, true]);
    }

    pub(crate) fn discard_request(streams: &[&str]) -> StartJobRequest {
        let mut req = request(streams);
        req.edges.pop();
        req.sink = Some(SinkPlan {
            sink: Some(sink_plan::Sink::Discard(DiscardSink {})),
        });
        req
    }

    #[test]
    fn names_tables_keeping_restored_names() {
        let mut req = request(&["Users", "users", "1st"]);
        let restored = arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new())
            .with_metadata(HashMap::from([(TABLE_KEY.to_owned(), "users".to_owned())]));
        req.streams[1].restored_schema = crate::ipc::encode_schema(&restored).unwrap();
        let p = Plan::from_request(req).unwrap();
        assert_eq!(p.tables, vec!["users_2", "users", "_1st"]);

        let mut req = request(&["a", "b"]);
        let taken = arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new()).with_metadata(
            HashMap::from([(TABLE_KEY.to_owned(), "_openetl_loads".to_owned())]),
        );
        req.streams[0].restored_schema = crate::ipc::encode_schema(&taken).unwrap();
        assert!(Plan::from_request(req).is_err());
    }

    #[test]
    fn a_named_table_is_used_as_written_and_replaces_a_restored_one() {
        let mut req = request(&["orders", "Users"]);
        req.streams[0].table = "Fct-Orders".to_owned();
        let restored = arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new())
            .with_metadata(HashMap::from([(TABLE_KEY.to_owned(), "orders".to_owned())]));
        req.streams[0].restored_schema = crate::ipc::encode_schema(&restored).unwrap();
        let p = Plan::from_request(req).unwrap();
        assert_eq!(p.tables, vec!["Fct-Orders", "users"]);

        let mut req = request(&["a", "b"]);
        req.streams[0].table = "same".to_owned();
        req.streams[1].table = "same".to_owned();
        assert!(Plan::from_request(req).is_err());

        let mut req = request(&["a"]);
        req.streams[0].table = "_openetl_loads".to_owned();
        assert!(Plan::from_request(req).is_err());
    }

    #[test]
    fn a_parquet_sink_has_no_outbound_edge() {
        let p = Plan::from_request(parquet_request(&["a", "b"], "file:///tmp/x")).unwrap();
        assert!(matches!(p.sink, Sink::Parquet { url, .. } if url == "file:///tmp/x"));
    }

    pub(crate) fn staged_request(streams: &[&str], url: &str) -> StartJobRequest {
        let mut req = request(streams);
        req.edges.pop();
        req.sink = Some(SinkPlan {
            sink: Some(sink_plan::Sink::Staged(StagedSink {
                url: url.into(),
                dir: String::new(),
            })),
        });
        req
    }

    #[test]
    fn a_staged_sink_has_no_outbound_edge() {
        let p = Plan::from_request(staged_request(&["a", "b"], "s3://bucket/x")).unwrap();
        assert!(matches!(p.sink, Sink::Staged { url, .. } if url == "s3://bucket/x"));
        let mut req = staged_request(&["a", "b"], "s3://bucket/x");
        req.edges
            .push(edge("out", EdgeDirection::FromEngine, &["a", "b"]));
        assert!(Plan::from_request(req).is_err());
    }

    #[test]
    fn a_discard_sink_has_no_outbound_edge() {
        let p = Plan::from_request(discard_request(&["a", "b"])).unwrap();
        assert!(matches!(p.sink, Sink::Discard));
        let mut req = discard_request(&["a", "b"]);
        req.edges
            .push(edge("out", EdgeDirection::FromEngine, &["a", "b"]));
        assert!(Plan::from_request(req).is_err());
    }

    type Mutation = Box<dyn Fn(&mut StartJobRequest)>;

    #[test]
    fn rejects_bad_plans() {
        let cases: Vec<(&str, Mutation)> = vec![
            ("generation", Box::new(|r| r.generation = 0)),
            ("no streams", Box::new(|r| r.streams.clear())),
            (
                "repeated stream",
                Box::new(|r| r.streams[1].name = "a".into()),
            ),
            (
                "declared schema",
                Box::new(|r| r.streams[0].arrow_schema = vec![1]),
            ),
            (
                "declared type",
                Box::new(|r| {
                    let s = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        "x",
                        arrow_schema::DataType::UInt64,
                        true,
                    )]);
                    r.streams[0].arrow_schema = crate::ipc::encode_schema(&s).unwrap();
                }),
            ),
            (
                "restored schema",
                Box::new(|r| r.streams[0].restored_schema = vec![1]),
            ),
            ("two lanes", Box::new(|r| r.lanes = 2)),
            ("two writer lanes", Box::new(|r| r.writer_lanes = 2)),
            (
                "key groups not a power of two",
                Box::new(|r| r.key_groups = 100),
            ),
            ("an edge on lane 1", Box::new(|r| r.edges[0].lane = 1)),
            (
                "routed by key group without a key",
                Box::new(|r| r.streams[0].routing = Routing::KeyGroup as i32),
            ),
            (
                "restored snapshot",
                Box::new(|r| {
                    r.restored_snapshots.push(StateSnapshot {
                        operator: "window".into(),
                        url: "file:///x".into(),
                        key_groups: None,
                    })
                }),
            ),
            ("no sink", Box::new(|r| r.sink = None)),
            (
                "parquet with an outbound edge",
                Box::new(|r| {
                    *r = parquet_request(&["a", "b"], "file:///tmp/x");
                    r.edges
                        .push(edge("out", EdgeDirection::FromEngine, &["a", "b"]));
                }),
            ),
            (
                "parquet URL",
                Box::new(|r| *r = parquet_request(&["a", "b"], "s3://bucket/x")),
            ),
            (
                "parquet directory",
                Box::new(|r| {
                    *r = parquet_request(&["a", "b/c"], "file:///tmp/x");
                    r.naming = Some(openetl_proto::engine::v1::Naming {
                        keep_source_names: true,
                        max_length: 0,
                    });
                }),
            ),
            (
                "unknown stream",
                Box::new(|r| r.edges[0].streams.push("z".into())),
            ),
            (
                "stream fed twice",
                Box::new(|r| r.edges.push(edge("in2", EdgeDirection::ToEngine, &["a"]))),
            ),
            (
                "stream not fed",
                Box::new(|r| r.edges[0].streams.pop().map(drop).unwrap_or(())),
            ),
            (
                "partial sink edge",
                Box::new(|r| r.edges[1].streams.pop().map(drop).unwrap_or(())),
            ),
            (
                "other outbound",
                Box::new(|r| r.edges.push(edge("x", EdgeDirection::FromEngine, &[]))),
            ),
            ("no direction", Box::new(|r| r.edges[0].direction = 0)),
            ("repeated edge", Box::new(|r| r.edges[1].id = "in".into())),
        ];
        for (name, mutate) in cases {
            let mut req = request(&["a", "b"]);
            mutate(&mut req);
            let err = Plan::from_request(req).unwrap_err();
            assert_eq!(
                err.code,
                openetl_proto::connector::v1::ErrorCode::ConfigInvalid,
                "{name}"
            );
        }
    }
}
