use crate::application::{adapter_runtime::AdapterCallError, providers::ProviderLaunchError};

#[derive(Debug, thiserror::Error)]
pub enum AdapterProcessError {
    #[error(transparent)]
    Provider(#[from] ProviderLaunchError),
    #[error("adapter executable could not be started: {0}")]
    Io(#[from] std::io::Error),
    #[error("adapter bootstrap message is invalid: {0}")]
    BootstrapJson(#[from] serde_json::Error),
    #[error("adapter launch token generation failed: {0}")]
    Random(#[from] getrandom::Error),
    #[error("adapter process instance ID is invalid")]
    InvalidProcessInstanceId,
    #[error("adapter child {0} pipe is unavailable")]
    MissingChildPipe(&'static str),
    #[error("adapter did not become ready before the deadline")]
    ReadyTimeout,
    #[error("adapter exited without a ready message")]
    MissingReadyMessage,
    #[error("adapter ready message exceeds the size limit")]
    ReadyMessageTooLarge,
    #[error("adapter ready message failed validation")]
    InvalidReadyMessage,
    #[error("adapter launch token is invalid")]
    InvalidLaunchToken,
    #[error("adapter RPC {0} exceeded its deadline")]
    RpcTimeout(&'static str),
    #[error("adapter transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("adapter RPC failed: {0}")]
    Rpc(#[source] tonic::Status),
    #[error("adapter handshake failed validation")]
    InvalidHandshake,
    #[error("adapter activation probe did not shut down gracefully")]
    ProbeShutdown,
    #[error("adapter response failed validation: {0}")]
    InvalidResponse(&'static str),
    #[error("adapter rejected the request: {0}")]
    Adapter(#[source] Box<AdapterCallError>),
    #[error("adapter connection is not open")]
    ConnectionNotOpen,
    #[error("adapter operation ID is invalid")]
    InvalidOperationId,
    #[error("adapter read request is invalid")]
    InvalidReadRequest,
    #[error("adapter targeted state-read request is invalid")]
    InvalidTargetedStateReadRequest,
    #[error("adapter targeted state-write request is invalid")]
    InvalidTargetedStateWriteRequest,
    #[error("adapter binary-asset request is invalid")]
    InvalidAssetRequest,
    #[error("adapter authentication request is invalid")]
    InvalidAuthenticationRequest,
    #[error("adapter exited unsuccessfully with status {0:?}")]
    UnexpectedExit(Option<i32>),
}

impl From<tonic::Status> for AdapterProcessError {
    fn from(status: tonic::Status) -> Self {
        Self::Rpc(status)
    }
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};

    use super::AdapterProcessError;

    #[test]
    fn rpc_error_preserves_provider_status_context() {
        let error = AdapterProcessError::from(Status::unavailable("Plex request timed out"));

        let AdapterProcessError::Rpc(status) = error else {
            panic!("expected RPC error");
        };
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "Plex request timed out");
    }
}
