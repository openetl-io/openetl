use std::collections::{HashMap, HashSet};

use arrow_schema::SchemaRef;
use openetl_normalize::Identity;
use openetl_normalize::naming::{Namer, Naming, RESERVED_TABLES, TABLE_KEY};
use openetl_proto::engine::v1::{EdgeDirection, StartJobRequest, sink_plan};

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
    /// Each stream's schema in the restored checkpoint.
    pub restored: Vec<Option<SchemaRef>>,
    /// Each stream's declared logical types, under source names.
    pub declared: Vec<Option<SchemaRef>>,
    pub identities: Vec<Identity>,
    pub quarantine: Option<std::sync::Arc<Quarantine>>,
    pub inbound: Vec<EdgePlan>,
    pub sink: Sink,
}

#[derive(Debug)]
pub enum Sink {
    /// A Go sink in the runner, fed by an edge that carries every stream.
    Runner {
        edge: EdgePlan,
        /// Each stream's index on the edge.
        out_index: Vec<u32>,
    },
    Parquet {
        url: String,
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
        let naming = req.naming.map_or_else(Naming::default, |n| Naming {
            keep_source: n.keep_source_names,
            max_len: n.max_length as usize,
        });
        let mut namer = Namer::new(naming, &RESERVED_TABLES);
        for (name, schema) in names.iter().zip(&restored) {
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
        let sink_edge = match &sink {
            sink_plan::Sink::Runner(r) => Some(r.edge.as_str()),
            sink_plan::Sink::Parquet(p) => {
                openetl_formats::check(&p.url, &tables)
                    .map_err(|e| JobError::config(e.to_string()))?;
                None
            }
            sink_plan::Sink::Discard(_) => None,
        };

        let mut ids = HashSet::new();
        let mut inbound = Vec::new();
        let mut outbound = None;
        let mut fed = vec![0; req.streams.len()];
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
            match e.direction() {
                EdgeDirection::ToEngine => {
                    for &i in &edge.streams {
                        fed[i] += 1;
                    }
                    inbound.push(edge);
                }
                EdgeDirection::FromEngine if Some(e.id.as_str()) == sink_edge => {
                    outbound = Some(edge)
                }
                EdgeDirection::FromEngine => {
                    return Err(JobError::config(format!(
                        "edge {} leaves the engine but isn't the sink's",
                        e.id
                    )));
                }
                EdgeDirection::Unspecified => {
                    return Err(JobError::config(format!("edge {} has no direction", e.id)));
                }
            }
        }
        if let Some(i) = fed.iter().position(|&n| n != 1) {
            return Err(JobError::config(format!(
                "stream {:?} must arrive on exactly one edge",
                req.streams[i].name
            )));
        }
        let sink = match sink {
            sink_plan::Sink::Runner(r) => {
                let edge = outbound.ok_or_else(|| {
                    JobError::config(format!("the sink's edge {:?} isn't planned", r.edge))
                })?;
                if edge.streams.len() != names.len() {
                    return Err(JobError::config("the sink's edge must carry every stream"));
                }
                let mut out_index = vec![0; names.len()];
                for (pos, &i) in edge.streams.iter().enumerate() {
                    out_index[i] = pos as u32;
                }
                Sink::Runner { edge, out_index }
            }
            sink_plan::Sink::Parquet(p) => Sink::Parquet { url: p.url },
            sink_plan::Sink::Discard(_) => Sink::Discard,
        };

        Ok(Plan {
            generation: req.generation,
            first_epoch: req.first_epoch,
            streams: names,
            tables,
            naming,
            restored,
            declared,
            identities,
            quarantine,
            inbound,
            sink,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(crate) mod tests {
    use super::*;
    use openetl_proto::engine::v1::{
        DiscardSink, Edge, ParquetSink, RunnerSink, SinkPlan, StateSnapshot, StreamPlan,
    };

    fn edge(id: &str, dir: EdgeDirection, streams: &[&str]) -> Edge {
        Edge {
            id: id.into(),
            direction: dir as i32,
            streams: streams.iter().map(|s| s.to_string()).collect(),
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
                sink: Some(sink_plan::Sink::Runner(RunnerSink { edge: "out".into() })),
            }),
            limits: None,
            restored_snapshots: Vec::new(),
            naming: None,
            quarantine_url: String::new(),
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
    fn maps_streams_to_edges() {
        let p = Plan::from_request(request(&["a", "b", "c"])).unwrap();
        assert_eq!(p.inbound.len(), 1);
        assert_eq!(p.inbound[0].streams, vec![0, 1, 2]);
        let Sink::Runner { edge, out_index } = p.sink else {
            panic!("{:?}", p.sink)
        };
        assert_eq!(edge.streams, vec![2, 1, 0]);
        assert_eq!(out_index, vec![2, 1, 0]);
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
    fn a_parquet_sink_has_no_outbound_edge() {
        let p = Plan::from_request(parquet_request(&["a", "b"], "file:///tmp/x")).unwrap();
        assert!(matches!(p.sink, Sink::Parquet { url } if url == "file:///tmp/x"));
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
            (
                "restored snapshot",
                Box::new(|r| {
                    r.restored_snapshots.push(StateSnapshot {
                        operator: "window".into(),
                        url: "file:///x".into(),
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
