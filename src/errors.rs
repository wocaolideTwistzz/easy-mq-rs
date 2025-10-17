use bincode::error::{DecodeError, EncodeError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    EncodeError(#[from] EncodeError),

    #[error(transparent)]
    DecodeError(#[from] DecodeError),
}

pub type Result<T> = std::result::Result<T, crate::errors::Error>;
