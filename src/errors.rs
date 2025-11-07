use bincode::error::{DecodeError, EncodeError};
use deadpool_redis::{BuildError, ConfigError, PoolError, redis::RedisError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    EncodeError(#[from] EncodeError),

    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    RedisError(#[from] RedisError),

    #[error(transparent)]
    RedisPoolError(#[from] PoolError),

    #[error(transparent)]
    RedisConfigError(#[from] ConfigError),

    #[error(transparent)]
    RedisBuildError(#[from] BuildError),

    #[error("task already exists")]
    TaskAlreadyExists,

    #[error("unsupported task state: {0}")]
    UnsupportedTaskState(String),

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

    #[error("invalid queue name")]
    InvalidQueueName,

    #[error("invalid topic name")]
    InvalidTopicName,
}

pub type Result<T> = std::result::Result<T, crate::errors::Error>;
