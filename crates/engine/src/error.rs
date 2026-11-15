use openetl_proto::connector::v1::{Error as ProtoError, ErrorCode};
use openetl_proto::engine::v1::PeerLost;
use prost::Message;
use tonic::{Code, Status};

/// Why a job failed. The message never contains record contents.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{}: {message}", code.as_str_name())]
pub struct JobError {
    pub code: ErrorCode,
    pub message: String,
    /// The runner closed its side of the job, so the engine exits.
    pub runner_gone: bool,
    /// A lane found its writer lane gone, whose own error says why.
    pub stopped: bool,
    /// The job lost this engine of its mesh, whose own failure, if it had
    /// one, says why.
    pub peer_lost: Option<u32>,
}

impl JobError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            runner_gone: false,
            stopped: false,
            peer_lost: None,
        }
    }

    /// The job lost another engine of its mesh, `engine`: transient.
    pub fn peer_lost(engine: usize, message: impl Into<String>) -> Self {
        Self {
            peer_lost: u32::try_from(engine).ok(),
            ..Self::new(ErrorCode::Transient, message)
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ConfigInvalid, message)
    }

    pub fn runner_gone(message: impl Into<String>) -> Self {
        Self {
            runner_gone: true,
            ..Self::internal(message)
        }
    }

    pub fn stopped(message: impl Into<String>) -> Self {
        Self {
            stopped: true,
            ..Self::internal(message)
        }
    }

    /// A status carrying the error in google.rpc.Status details, with a
    /// PeerLost where the job lost another engine.
    pub fn to_status(&self) -> Status {
        let code = match self.code {
            ErrorCode::ConfigInvalid | ErrorCode::DataInvalid => Code::InvalidArgument,
            ErrorCode::ResourceExhausted => Code::ResourceExhausted,
            ErrorCode::SchemaDrift => Code::FailedPrecondition,
            _ => Code::Internal,
        };
        let detail = ProtoError {
            code: self.code as i32,
            message: self.message.clone(),
            retry_after: None,
        };
        let mut details = vec![prost_types::Any {
            type_url: "type.googleapis.com/openetl.connector.v1.Error".into(),
            value: detail.encode_to_vec(),
        }];
        if let Some(engine) = self.peer_lost {
            details.push(prost_types::Any {
                type_url: "type.googleapis.com/openetl.engine.v1.PeerLost".into(),
                value: PeerLost { engine }.encode_to_vec(),
            });
        }
        let details = RpcStatus {
            code: code as i32,
            message: self.message.clone(),
            details,
        };
        Status::with_details(code, self.message.clone(), details.encode_to_vec().into())
    }
}

/// google.rpc.Status
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn status_details_carry_the_taxonomy_error() {
        let st = JobError::new(ErrorCode::DataInvalid, "row 3 is not a JSON object").to_status();
        assert_eq!(st.code(), Code::InvalidArgument);
        let rpc = RpcStatus::decode(st.details()).unwrap();
        assert_eq!(rpc.details.len(), 1);
        assert_eq!(
            rpc.details[0].type_url,
            "type.googleapis.com/openetl.connector.v1.Error"
        );
        let detail = ProtoError::decode(rpc.details[0].value.as_slice()).unwrap();
        assert_eq!(detail.code(), ErrorCode::DataInvalid);
        assert_eq!(detail.message, "row 3 is not a JSON object");
    }

    #[test]
    fn a_lost_peer_is_in_the_details_too() {
        let st = JobError::peer_lost(2, "engine 2 was lost").to_status();
        let rpc = RpcStatus::decode(st.details()).unwrap();
        let types: Vec<&str> = rpc.details.iter().map(|d| d.type_url.as_str()).collect();
        assert_eq!(
            types,
            [
                "type.googleapis.com/openetl.connector.v1.Error",
                "type.googleapis.com/openetl.engine.v1.PeerLost"
            ]
        );
        let detail = ProtoError::decode(rpc.details[0].value.as_slice()).unwrap();
        assert_eq!(detail.code(), ErrorCode::Transient);
        assert_eq!(
            PeerLost::decode(rpc.details[1].value.as_slice())
                .unwrap()
                .engine,
            2
        );
    }
}
