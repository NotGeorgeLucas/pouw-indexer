//! Errors, one enum per layer. A fault of ours (the database) and bad data from the node are
//! different variants, because they are handled differently: the first is logged as our
//! problem, the second as the node's, and a node is never blamed for what the database did.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0} is not set")]
    Missing(&'static str),
    #[error("{var}: {reason}")]
    Invalid { var: &'static str, reason: String },
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migrations: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// The node sent data that does not add up. Not a fault of the database.
    #[error("bad data from the node: {0}")]
    BadData(String),
}

/// Talking to a node: the request itself, or an answer that is not what the API promises.
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("{path}: the node answered {status}: {body}")]
    Status {
        path: String,
        status: u16,
        body: String,
    },
    #[error("the node has no blocks")]
    NoBlocks,
    /// A fake node that fails on purpose, in tests.
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the node sent a broken page of blocks: {0}")]
    BadPage(String),
}

impl SyncError {
    /// True when the fault is on our side (database), not the node's.
    pub fn is_local_fault(&self) -> bool {
        matches!(
            self,
            Self::Store(StoreError::Db(_) | StoreError::Migrate(_))
        )
    }
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the node broke the event contract: {0}")]
    Contract(String),
}

/// An event of a known kind whose content does not make sense. The event is kept raw.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct BadEvent(pub String);
