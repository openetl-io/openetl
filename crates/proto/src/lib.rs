//! Generated types for the connector protocol (`openetl.connector.v1`) and
//! the runner–engine protocol (`openetl.engine.v1`). The `.proto` sources are
//! in `proto/` at the repository root.

pub mod connector {
    pub mod v1 {
        tonic::include_proto!("openetl.connector.v1");
    }
}

pub mod engine {
    pub mod v1 {
        tonic::include_proto!("openetl.engine.v1");
    }
}

impl connector::v1::ErrorCode {
    /// Whether an attempt that failed with this code is retried. `Internal`
    /// is retried a limited number of times, then raises an incident.
    ///
    /// The match is exhaustive, so a code added to the protocol doesn't build
    /// until it has a decision here, as in `internal/platform/errs`.
    pub fn retryable(self) -> bool {
        use connector::v1::ErrorCode as C;
        match self {
            C::Transient
            | C::RateLimited
            | C::AuthExpired
            | C::SchemaDrift
            | C::DataInvalid
            | C::DestinationUnavailable
            | C::ResourceExhausted
            | C::Internal => true,
            C::Unspecified
            | C::AuthInvalid
            | C::PermissionDenied
            | C::ConfigInvalid
            | C::ContractChanged
            | C::DestinationQuota
            | C::Fenced => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::connector::v1::ErrorCode;

    #[test]
    fn retryable_matches_the_taxonomy() {
        assert!(ErrorCode::RateLimited.retryable());
        assert!(!ErrorCode::AuthInvalid.retryable());
        assert!(!ErrorCode::Fenced.retryable());
        assert!(!ErrorCode::Unspecified.retryable());
    }
}
