use std::error::Error as StdError;
use std::io::Error as IoError;

use crate::database::StorageDatabaseError;

/// Non UTF-8 symbol in path.
#[derive(thiserror::Error, Debug)]
#[error("non-UTF-8 symbol in path")]
pub struct NonUtf8PathError;

/// Carol storage error.
#[derive(thiserror::Error, Debug)]
pub enum StorageError<E: StorageDatabaseError> {
    /// Storage database releated error
    #[error("database error")]
    DatabaseError(#[from] E),

    /// Some I/O operation failed
    #[error("I/O error")]
    IoError(#[from] IoError),

    /// Failed to wait for file to be downloaded
    ///
    /// This happens when one thread is waiting for
    /// a file, which is currently being downloaded by another thread,
    /// and that download fails.
    #[error("awaited file failed to download")]
    AwaitingError,

    #[error(transparent)]
    NonUtf8PathError(#[from] NonUtf8PathError),

    #[error(transparent)]
    CustomError(Box<dyn StdError + Send + 'static>),

    #[error("storage directory path is not absolute")]
    StorageDirectoryPathIsNotAbsolute,

    #[error("failed to evict files from storage")]
    EvictionFailed,
}

impl<E: StorageDatabaseError> StorageError<E> {
    pub fn custom<T: StdError + 'static + Send>(error: T) -> Self {
        Self::CustomError(Box::new(error))
    }
}

// TODO: refactor error type (remove generic param)
