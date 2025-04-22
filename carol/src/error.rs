//! Carol errors.

use std::error::Error as StdError;
use std::fmt;
use std::io::Error as IoError;

use crate::database::StorageDatabaseError;
use crate::file::FileLockError;

pub type BoxError = Box<dyn StdError + Send + Sync>;

/// Error type returned from a storage manager.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: Option<BoxError>,
}

impl Error {
    /// Get kind of the error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Create error of kind `AwaitingError`.
    pub fn awaiting() -> Self {
        Self {
            kind: ErrorKind::AwaitingError,
            source: None,
        }
    }

    /// Create error of kind `EvictionError`.
    pub fn eviction(error: BoxError) -> Self {
        Self {
            kind: ErrorKind::EvictionError,
            source: Some(error),
        }
    }

    /// Create error of kind `InitializationError`.
    pub fn init(error: BoxError) -> Self {
        Self {
            kind: ErrorKind::InitializationError,
            source: Some(error),
        }
    }

    /// Create error of kind `Other`.
    pub fn other(error: BoxError) -> Self {
        Self {
            kind: ErrorKind::Other,
            source: Some(error),
        }
    }

    /// Returns `true` if the error is [`FreeSpaceError`].
    pub fn is_free_space_error(&self) -> bool {
        self.kind == ErrorKind::EvictionError
            && self
                .source
                .as_ref()
                .is_some_and(|source| source.downcast_ref::<FreeSpaceError>().is_some())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.kind {
            ErrorKind::DatabaseError => "storage database error",
            ErrorKind::IoError => "I/O error",
            ErrorKind::AwaitingError => "failed to wait for file to become available",
            ErrorKind::InitializationError => "failed to initialize storage manager",
            ErrorKind::EvictionError => "failed to free space in storage",
            ErrorKind::FileLockError => "file locking error",
            ErrorKind::Other => "custom error",
        })
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|err| &**err as _)
    }
}

impl<T: StorageDatabaseError + 'static> From<T> for Error {
    fn from(error: T) -> Self {
        Self {
            kind: ErrorKind::DatabaseError,
            source: Some(Box::new(error)),
        }
    }
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Self {
        Self {
            kind: ErrorKind::IoError,
            source: Some(Box::new(error)),
        }
    }
}

impl From<FileLockError> for Error {
    fn from(error: FileLockError) -> Self {
        Self {
            kind: ErrorKind::FileLockError,
            source: Some(Box::new(error)),
        }
    }
}

/// Kind of the [`Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Storage database releated error
    DatabaseError,

    /// Some I/O operation failed
    IoError,

    /// Failed to wait for file to be downloaded
    ///
    /// This happens when one thread is waiting for a file which is currently being downloaded by
    /// another thread and that download fails.
    AwaitingError,

    /// Storage initialization error.
    InitializationError,

    /// Eviction related error.
    EvictionError,

    /// File locking error.
    FileLockError,

    /// Other error.
    Other,
}

/// Non UTF-8 symbol in path.
#[derive(thiserror::Error, Debug, Clone, Copy)]
#[error("non-UTF-8 symbol in path")]
pub struct NonUtf8PathError;

/// Failed to find a file to remove.
/// This most likely means that all the files were locked during eviction.
#[derive(thiserror::Error, Debug, Clone, Copy)]
#[error("failed to find a file to remove")]
pub struct FreeSpaceError;

/// Storage directory path is not absolute.
#[derive(thiserror::Error, Debug, Clone, Copy)]
#[error("storage directory path is not absolute")]
pub struct StoragePathError;
