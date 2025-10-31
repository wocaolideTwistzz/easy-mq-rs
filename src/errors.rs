use bincode::error::{DecodeError, EncodeError};
use deadpool_redis::redis::RedisError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    EncodeError(#[from] EncodeError),

    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    RedisError(#[from] RedisError),

    #[error("task already exists")]
    TaskAlreadyExists,

    #[error("unknown task state: {0}")]
    UnknownTaskState(String),

    #[error("scheduled at time not set")]
    ScheduledAtTimeNotSet,

    #[error("dependent tasks not set")]
    DependentTasksNotSet,

    #[error("missing stream id")]
    MissingStreamID,

    #[error("retry not set")]
    RetryNotSet,

    #[error("retry has exceeded")]
    RetryHasExceeded,

    #[error("task not found")]
    TaskNotFound,
}

pub type Result<T> = std::result::Result<T, crate::errors::Error>;
