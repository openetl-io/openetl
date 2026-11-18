//! Each stream's schema where several engines run one attempt: one owner
//! outside them decides it, and the runner reaches it through the Schemas
//! call. That's each of the destination's streams' schema, with the columns
//! it holds whole, and each stream a transform reads. A lane that changes a
//! stream's schema proposes it from the owner's version its engine follows,
//! one proposal of a stream at a time, and waits for a newer version; its
//! engine then shapes by the newest. Once the owner has sealed a stream, as
//! it merges the shares of the first epoch with its columns, its versions
//! are sealed: every engine shapes by them under the schema policy, whether
//! or not its own lanes have had rows. Once the call fails, every proposal waiting and
//! every lane that shapes next fails the job with why.

use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use openetl_proto::engine::v1::{ColumnHold, SchemasRequest, SchemasResponse};
use tokio::sync::{mpsc, watch};
use tonic::Status;

use crate::error::JobError;
use crate::ipc;

pub type Proposals = mpsc::Receiver<Result<SchemasResponse, Status>>;

pub struct Remote {
    streams: Vec<RemoteStream>,
    inputs: Vec<RemoteStream>,
    /// Taken by the Schemas call.
    proposals: Mutex<Option<Proposals>>,
    failure: Arc<watch::Sender<Option<JobError>>>,
}

/// A stream's version as its owner has it.
#[derive(Clone, Debug, Default)]
pub struct Owned {
    pub version: u64,
    pub schema: Option<SchemaRef>,
    /// The columns one of the destination's streams holds whole.
    pub holding: Vec<ColumnHold>,
    /// The schema policy applies to the version, as SchemasRequest.sealed
    /// says.
    pub sealed: bool,
}

/// What a change to one of the destination's streams holds, beside its
/// schema: the columns held whole, and the schema the destination gets, as
/// an Arrow IPC stream holding only the schema, where that isn't the
/// stream's; empty where it is. Empty for a stream a transform reads.
#[derive(Default)]
pub struct Holding {
    pub columns: Vec<ColumnHold>,
    pub delivered: Vec<u8>,
}

pub struct RemoteStream {
    stream: u32,
    input: bool,
    proposals: mpsc::Sender<Result<SchemasResponse, Status>>,
    /// The owner's newest version, and what it holds.
    newest: watch::Sender<u64>,
    latest: Mutex<Owned>,
    /// Held while a proposal of the stream waits for its answer.
    turn: tokio::sync::Mutex<()>,
    /// Why the Schemas call failed; None while it works.
    failure: Arc<watch::Sender<Option<JobError>>>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("streams", &self.streams.len())
            .field("inputs", &self.inputs.len())
            .finish_non_exhaustive()
    }
}

impl Remote {
    /// For `streams` of the destination's, and `inputs` a transform reads.
    pub fn new(streams: usize, inputs: usize) -> Self {
        let (tx, rx) = mpsc::channel((streams + inputs).max(1));
        let failure = Arc::new(watch::Sender::new(None));
        let make = |n: usize, input: bool| -> Vec<RemoteStream> {
            (0..n)
                .map(|s| RemoteStream {
                    stream: u32::try_from(s).unwrap_or(u32::MAX),
                    input,
                    proposals: tx.clone(),
                    newest: watch::Sender::new(0),
                    latest: Mutex::new(Owned::default()),
                    turn: tokio::sync::Mutex::new(()),
                    failure: failure.clone(),
                })
                .collect()
        };
        Self {
            streams: make(streams, false),
            inputs: make(inputs, true),
            proposals: Mutex::new(Some(rx)),
            failure,
        }
    }

    /// The Schemas call failed with e, unless it had already.
    pub fn fail(&self, e: JobError) {
        self.failure.send_if_modified(|f| {
            if f.is_some() {
                return false;
            }
            *f = Some(e);
            true
        });
    }

    /// One of the destination's streams.
    pub fn stream(&self, s: usize) -> Option<&RemoteStream> {
        self.streams.get(s)
    }

    /// One of the streams a transform reads.
    pub fn input(&self, s: usize) -> Option<&RemoteStream> {
        self.inputs.get(s)
    }

    /// Sets what version 0 of a stream holds, before any update: the
    /// schema the job restored, which the owner starts from too.
    pub fn start(&self, input: bool, s: usize, schema: Option<SchemaRef>) {
        let streams = match input {
            true => &self.inputs,
            false => &self.streams,
        };
        if let Some(st) = streams.get(s) {
            let mut latest = st.latest.lock().unwrap_or_else(|e| e.into_inner());
            if latest.version == 0 {
                latest.schema = schema;
            }
        }
    }

    /// The proposals, for the one Schemas call.
    pub fn take_proposals(&self) -> Option<Proposals> {
        self.proposals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// Takes the owner's version of a stream; an older one than the engine
    /// has is dropped.
    pub fn update(&self, u: &SchemasRequest) -> Result<(), Status> {
        let streams = match u.input {
            true => &self.inputs,
            false => &self.streams,
        };
        let s = streams
            .get(u.stream as usize)
            .ok_or_else(|| Status::invalid_argument(format!("no stream {}", u.stream)))?;
        let schema = ipc::decode_schema(&u.arrow_schema)
            .map_err(|e| Status::invalid_argument(format!("stream {}'s schema: {e}", u.stream)))?;
        let mut latest = s.latest.lock().unwrap_or_else(|e| e.into_inner());
        if u.version > latest.version {
            *latest = Owned {
                version: u.version,
                schema: Some(schema),
                holding: u.holding.clone(),
                sealed: u.sealed,
            };
            s.newest.send_replace(u.version);
        }
        Ok(())
    }
}

impl RemoteStream {
    pub fn newest(&self) -> u64 {
        *self.newest.borrow()
    }

    pub fn latest(&self) -> Owned {
        self.latest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Why the Schemas call failed, if it has.
    pub fn check(&self) -> Result<(), JobError> {
        match &*self.failure.borrow() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Proposes schema, made from version base, unless the owner has moved
    /// on from it since, and returns once the owner has a newer version, or
    /// the Schemas call fails.
    pub async fn propose(
        &self,
        base: u64,
        schema: &SchemaRef,
        holding: Holding,
    ) -> Result<(), JobError> {
        let _turn = self.turn.lock().await;
        self.check()?;
        if self.newest() > base {
            return Ok(());
        }
        let arrow_schema =
            ipc::encode_schema(schema).map_err(|e| JobError::internal(e.to_string()))?;
        let proposal = SchemasResponse {
            stream: self.stream,
            base,
            arrow_schema,
            input: self.input,
            holding: holding.columns,
            delivered: holding.delivered,
        };
        self.proposals
            .send(Ok(proposal))
            .await
            .map_err(|_| JobError::runner_gone("the runner closed the schemas call"))?;
        let mut newer = self.newest.subscribe();
        let mut failed = self.failure.subscribe();
        tokio::select! {
            r = newer.wait_for(|v| *v > base) => {
                r.map(|_| ()).map_err(|_| JobError::runner_gone("the schemas call ended"))
            }
            _ = failed.wait_for(Option::is_some) => self.check(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema(cols: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            cols.iter()
                .map(|c| Field::new(*c, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn update(stream: u32, version: u64, s: &SchemaRef) -> SchemasRequest {
        SchemasRequest {
            stream,
            version,
            arrow_schema: ipc::encode_schema(s).unwrap(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_proposal_waits_for_a_newer_version() {
        let r = Arc::new(Remote::new(2, 0));
        let mut proposals = r.take_proposals().unwrap();
        assert!(r.take_proposals().is_none());
        let s = schema(&["a"]);
        let waiting = {
            let (r, s) = (r.clone(), s.clone());
            tokio::spawn(async move {
                r.stream(1)
                    .unwrap()
                    .propose(0, &s, Holding::default())
                    .await
            })
        };
        let p = proposals.recv().await.unwrap().unwrap();
        assert_eq!((p.stream, p.base, p.input), (1, 0, false));
        assert!(!waiting.is_finished());
        r.update(&update(1, 1, &s)).unwrap();
        waiting.await.unwrap().unwrap();
        assert_eq!(r.stream(1).unwrap().latest().version, 1);
        // An older version than the engine has is dropped, and a proposal
        // from a version the owner has moved on from isn't sent.
        r.update(&update(1, 1, &schema(&["b"]))).unwrap();
        assert_eq!(
            r.stream(1)
                .unwrap()
                .latest()
                .schema
                .unwrap()
                .field(0)
                .name(),
            "a"
        );
        r.stream(1)
            .unwrap()
            .propose(0, &s, Holding::default())
            .await
            .unwrap();
        assert!(proposals.try_recv().is_err());
        assert!(r.update(&update(2, 1, &s)).is_err());
    }

    // A version says whether the owner has sealed it.
    #[test]
    fn a_version_carries_its_seal() {
        let r = Remote::new(1, 0);
        let s = schema(&["a"]);
        let stream = r.stream(0).unwrap();
        r.update(&update(0, 1, &s)).unwrap();
        assert!(!stream.latest().sealed);
        r.update(&SchemasRequest {
            sealed: true,
            ..update(0, 2, &s)
        })
        .unwrap();
        assert_eq!((stream.latest().version, stream.latest().sealed), (2, true));
    }

    // A stream a transform reads has versions of its own, apart from the
    // destination's stream of the same index, and so do its proposals.
    #[tokio::test]
    async fn inputs_are_owned_apart() {
        let r = Arc::new(Remote::new(1, 1));
        let mut proposals = r.take_proposals().unwrap();
        let s = schema(&["a"]);
        let waiting = {
            let (r, s) = (r.clone(), s.clone());
            tokio::spawn(
                async move { r.input(0).unwrap().propose(0, &s, Holding::default()).await },
            )
        };
        let p = proposals.recv().await.unwrap().unwrap();
        assert_eq!((p.stream, p.input), (0, true));
        r.update(&update(0, 1, &s)).unwrap();
        assert!(!waiting.is_finished());
        assert_eq!(r.input(0).unwrap().newest(), 0);
        r.update(&SchemasRequest {
            input: true,
            ..update(0, 1, &s)
        })
        .unwrap();
        waiting.await.unwrap().unwrap();
        assert_eq!(r.input(0).unwrap().newest(), 1);
    }

    #[tokio::test]
    async fn a_failed_call_releases_proposals_with_why() {
        let r = Arc::new(Remote::new(1, 0));
        let mut proposals = r.take_proposals().unwrap();
        let s = schema(&["a"]);
        let waiting = {
            let (r, s) = (r.clone(), s.clone());
            tokio::spawn(async move {
                r.stream(0)
                    .unwrap()
                    .propose(0, &s, Holding::default())
                    .await
            })
        };
        proposals.recv().await.unwrap().unwrap();
        r.fail(JobError::internal("the owner is gone"));
        r.fail(JobError::internal("a later failure"));
        let err = waiting.await.unwrap().unwrap_err();
        assert_eq!(err.message, "the owner is gone");
        // Later proposals fail at once, and aren't sent.
        let err = r
            .stream(0)
            .unwrap()
            .propose(0, &s, Holding::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "the owner is gone");
        assert!(r.stream(0).unwrap().check().is_err());
        assert!(proposals.try_recv().is_err());
    }
}
