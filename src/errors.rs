use std::fmt::{Debug, Display};

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

    #[error("process task timeout")]
    ProcessTaskTimeout,

    #[error("disable retry")]
    DisableRetry,

    #[error("custom message {0}")]
    CustomMessage(String),

    #[error("custom error {0}")]
    Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub type Result<T> = std::result::Result<T, crate::errors::Error>;

impl Error {
    pub fn custom<M>(message: M) -> Error
    where
        M: Debug + Display + Send + Sync + 'static,
    {
        Error::CustomMessage(message.to_string())
    }
}

pub trait OptionExt<T> {
    fn ok_or_easy_mq<M>(self, message: M) -> Result<T>
    where
        M: Debug + Display + Send + Sync + 'static;
}

impl<T> OptionExt<T> for Option<T> {
    fn ok_or_easy_mq<M>(self, message: M) -> Result<T>
    where
        M: Debug + Display + Send + Sync + 'static,
    {
        match self {
            Some(ok) => Ok(ok),
            None => Err(Error::custom(message)),
        }
    }
}

impl<T> From<Box<T>> for Error
where
    T: std::error::Error + Send + Sync + 'static,
{
    fn from(value: Box<T>) -> Self {
        Error::Custom(value)
    }
}
