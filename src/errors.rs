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
}

pub type Result<T> = std::result::Result<T, crate::errors::Error>;
