//! Generated types for `openetl.connector.v1` and `openetl.engine.v1`.

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
    /// Whether an attempt that failed with this code is retried.
    pub fn retryable(self) -> bool {
        use connector::v1::ErrorCode as C;
        match self {
            C::Transient
            | C::RateLimited
            | C::AuthExpired
            | C::DataInvalid
            | C::DestinationUnavailable
            | C::ResourceExhausted
            | C::Internal
            | C::Draining => true,
            C::Unspecified
            | C::AuthInvalid
            | C::PermissionDenied
            | C::ConfigInvalid
            | C::SchemaDrift
            | C::ContractChanged
            | C::DestinationQuota
            | C::Fenced
            | C::PositionLost => false,
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
