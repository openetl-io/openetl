use openetl_proto::connector::v1::{Error as ProtoError, ErrorCode};
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
}

impl JobError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            runner_gone: false,
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

    /// A status carrying the error in google.rpc.Status details.
    pub fn to_status(&self) -> Status {
        let code = match self.code {
            ErrorCode::ConfigInvalid | ErrorCode::DataInvalid => Code::InvalidArgument,
            ErrorCode::ResourceExhausted => Code::ResourceExhausted,
            _ => Code::Internal,
        };
        let detail = ProtoError {
            code: self.code as i32,
            message: self.message.clone(),
            retry_after: None,
        };
        let details = RpcStatus {
            code: code as i32,
            message: self.message.clone(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/openetl.connector.v1.Error".into(),
                value: detail.encode_to_vec(),
            }],
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
}
