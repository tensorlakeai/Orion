use std::fmt;
use std::time::Duration;

use openraft::errors::{ClientWriteError, ForwardToLeader, RaftError};
use tokio::time::sleep;

use crate::{
    HybridClock, OrionRaft, OrionRaftRequest,
    openraft_store::OrionTypeConfig,
    tonic_transport::{
        ClientWriteRpcError, TonicRaftTransportConfig, client_write_to_raft_endpoint,
    },
};

const DEFAULT_LOCAL_APPLY_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_FORWARD_MAX_ATTEMPTS: usize = 4;
const DEFAULT_FORWARD_INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const DEFAULT_FORWARD_MAX_BACKOFF: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct OrionSqliteRaftClient {
    raft: Option<OrionRaft>,
    transport_config: TonicRaftTransportConfig,
    local_apply_timeout: Duration,
    forward_retry: ForwardRetryPolicy,
}

#[derive(Debug)]
pub enum OrionSqliteRaftError {
    Unavailable(String),
    NotLeader(String),
    Write(String),
    Transport(String),
    ApplyTimeout(String),
    Internal(String),
}

#[derive(Debug, Clone)]
struct ForwardRetryPolicy {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl fmt::Display for OrionSqliteRaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(message)
            | Self::NotLeader(message)
            | Self::Write(message)
            | Self::Transport(message)
            | Self::ApplyTimeout(message)
            | Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for OrionSqliteRaftError {}

impl OrionSqliteRaftClient {
    pub fn new(raft: Option<OrionRaft>) -> Self {
        Self::with_transport_config(raft, TonicRaftTransportConfig::default())
    }

    pub fn with_transport_config(
        raft: Option<OrionRaft>,
        transport_config: TonicRaftTransportConfig,
    ) -> Self {
        Self {
            raft,
            transport_config,
            local_apply_timeout: DEFAULT_LOCAL_APPLY_TIMEOUT,
            forward_retry: ForwardRetryPolicy::default(),
        }
    }

    pub async fn propose(
        &self,
        request: OrionRaftRequest,
    ) -> Result<Option<u64>, OrionSqliteRaftError> {
        self.retry_forwarding("SQLite Raft write", || async {
            self.propose_once(request.clone()).await
        })
        .await
    }

    async fn propose_once(
        &self,
        request: OrionRaftRequest,
    ) -> Result<Option<u64>, OrionSqliteRaftError> {
        let raft = self.raft.as_ref().ok_or_else(|| {
            OrionSqliteRaftError::Unavailable(
                "Raft writes are unavailable because this process has no Raft node".to_string(),
            )
        })?;
        let request = request.assign_commit_timestamp(HybridClock::global());
        match raft.client_write(request.clone()).await {
            Ok(response) => self.wait_for_local_apply(Some(response.log_id.index)).await,
            Err(err) => {
                if let Some(forward) = err.forward_to_leader() {
                    return self.forward_write_to_leader(forward, request).await;
                }
                Err(map_client_write_error(err))
            }
        }
    }

    async fn retry_forwarding<T, Fut>(
        &self,
        operation: &'static str,
        mut run: impl FnMut() -> Fut,
    ) -> Result<T, OrionSqliteRaftError>
    where
        Fut: std::future::Future<Output = Result<T, OrionSqliteRaftError>>,
    {
        let max_attempts = self.forward_retry.max_attempts.max(1);
        let mut backoff = self.forward_retry.initial_backoff;
        let mut last_error = None;

        for attempt in 1..=max_attempts {
            match run().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < max_attempts && error.is_retryable_forwarding_error() => {
                    last_error = Some(error);
                    sleep(backoff).await;
                    backoff = next_backoff(backoff, self.forward_retry.max_backoff);
                }
                Err(error) => return Err(error),
            }
        }

        let error = last_error.unwrap_or_else(|| {
            OrionSqliteRaftError::Internal(format!("{operation} retry loop exhausted unexpectedly"))
        });
        Err(error.with_retry_context(operation, max_attempts))
    }

    async fn forward_write_to_leader(
        &self,
        forward: &ForwardToLeader<OrionTypeConfig>,
        request: OrionRaftRequest,
    ) -> Result<Option<u64>, OrionSqliteRaftError> {
        let endpoint = leader_endpoint(forward)?;
        let response =
            client_write_to_raft_endpoint(endpoint, request, self.transport_config.clone())
                .await
                .map_err(map_forwarded_client_write_error)?;
        self.wait_for_local_apply(Some(response.log_id.index)).await
    }

    async fn wait_for_local_apply(
        &self,
        log_index: Option<u64>,
    ) -> Result<Option<u64>, OrionSqliteRaftError> {
        let Some(log_index) = log_index else {
            return Ok(None);
        };
        let raft = self.raft.as_ref().ok_or_else(|| {
            OrionSqliteRaftError::Unavailable(
                "Raft writes are unavailable because this process has no Raft node".to_string(),
            )
        })?;
        raft.wait(Some(self.local_apply_timeout))
            .applied_index_at_least(
                Some(log_index),
                "SQLite Raft write applied locally",
            )
            .await
            .map_err(|err| {
                OrionSqliteRaftError::ApplyTimeout(format!(
                    "write committed but this node did not apply it before returning to SQLite: {err}"
                ))
            })?;
        Ok(Some(log_index))
    }
}

impl OrionSqliteRaftError {
    fn is_retryable_forwarding_error(&self) -> bool {
        matches!(self, Self::NotLeader(_) | Self::Transport(_))
    }

    fn with_retry_context(self, operation: &str, attempts: usize) -> Self {
        match self {
            Self::NotLeader(message) => Self::NotLeader(format!(
                "{operation} did not find a reachable Raft leader after {attempts} attempt(s): {message}"
            )),
            Self::Transport(message) => Self::Transport(format!(
                "{operation} could not reach the Raft leader after {attempts} attempt(s): {message}"
            )),
            other => other,
        }
    }
}

impl Default for ForwardRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_FORWARD_MAX_ATTEMPTS,
            initial_backoff: DEFAULT_FORWARD_INITIAL_BACKOFF,
            max_backoff: DEFAULT_FORWARD_MAX_BACKOFF,
        }
    }
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

fn leader_endpoint(
    forward: &ForwardToLeader<OrionTypeConfig>,
) -> Result<String, OrionSqliteRaftError> {
    let Some(leader_node) = &forward.leader_node else {
        return Err(not_leader_error(forward));
    };
    if leader_node.addr.is_empty() {
        return Err(not_leader_error(forward));
    }
    Ok(leader_node.addr.clone())
}

fn map_client_write_error(
    error: RaftError<OrionTypeConfig, ClientWriteError<OrionTypeConfig>>,
) -> OrionSqliteRaftError {
    if let Some(forward) = error.forward_to_leader() {
        not_leader_error(forward)
    } else {
        OrionSqliteRaftError::Write(error.to_string())
    }
}

fn map_forwarded_client_write_error(error: ClientWriteRpcError) -> OrionSqliteRaftError {
    match error {
        ClientWriteRpcError::Raft(error) => map_client_write_error(error),
        ClientWriteRpcError::Transport(error) => OrionSqliteRaftError::Transport(format!(
            "failed to forward write to Raft leader: {error}"
        )),
    }
}

fn not_leader_error(forward: &ForwardToLeader<OrionTypeConfig>) -> OrionSqliteRaftError {
    let leader = match (&forward.leader_id, &forward.leader_node) {
        (Some(id), Some(node)) if !node.addr.is_empty() => {
            format!("leader is node {id} at {}", node.addr)
        }
        (Some(id), _) => format!("leader is node {id}, but no leader address is known"),
        _ => "leader is not known yet".to_string(),
    };
    OrionSqliteRaftError::NotLeader(format!(
        "this node is not the Raft leader and could not forward the request; {leader}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_forwarding_errors_are_classified_narrowly() {
        assert!(
            OrionSqliteRaftError::NotLeader("leader unknown".to_string())
                .is_retryable_forwarding_error()
        );
        assert!(
            OrionSqliteRaftError::Transport("connection refused".to_string())
                .is_retryable_forwarding_error()
        );
        assert!(
            !OrionSqliteRaftError::Write("proposal rejected".to_string())
                .is_retryable_forwarding_error()
        );
        assert!(
            !OrionSqliteRaftError::Unavailable("no raft node".to_string())
                .is_retryable_forwarding_error()
        );
    }

    #[test]
    fn retry_context_preserves_error_class_and_attempt_count() {
        let error = OrionSqliteRaftError::Transport("connection refused".to_string())
            .with_retry_context("SQLite Raft write", 4);
        assert!(matches!(error, OrionSqliteRaftError::Transport(_)));
        assert_eq!(
            error.to_string(),
            "SQLite Raft write could not reach the Raft leader after 4 attempt(s): connection refused"
        );
    }

    #[test]
    fn retry_backoff_caps_at_configured_maximum() {
        assert_eq!(
            next_backoff(Duration::from_millis(50), Duration::from_millis(500)),
            Duration::from_millis(100)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(400), Duration::from_millis(500)),
            Duration::from_millis(500)
        );
    }
}
