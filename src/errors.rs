//! Error types.

use std::fmt;
use std::io::Error as IoError;

use diesel::result::DatabaseErrorKind;
use eyre::eyre;

#[doc(no_inline)]
pub use diesel::result::{ConnectionError, Error as DieselError};

/// Cache database related errors.
#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("connection failed")]
    ConnectionError(#[from] ConnectionError),

    #[error("migration failed: {0}")]
    MigrationError(String),

    #[error(transparent)]
    DieselError(#[from] DieselError),

    #[error("failed to remove entry")]
    RemoveError(#[from] RemoveErrorReason),
}

/// Reason why remove failed.
#[derive(thiserror::Error, Debug)]
pub enum RemoveErrorReason {
    /// Reference counter is not 0
    #[error("reference counter is not 0")]
    UsedFile,

    /// Entry status is not 'ToRemove'
    #[error("entry status is not 'ToRemove'")]
    WrongStatus,
}

impl DatabaseError {
    /// Whether the error is unique key violation.
    pub fn is_unique_violation(&self) -> bool {
        matches!(
            *self,
            DatabaseError::DieselError(DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                _
            ))
        )
    }

    /// Whether the error is "not found".
    pub fn is_not_found(&self) -> bool {
        matches!(*self, DatabaseError::DieselError(DieselError::NotFound))
    }
}

/// Carol client error.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Cache database releated error
    #[error("database error")]
    DatabaseError(#[from] DatabaseError),

    /// Some I/O operation failed
    #[error("I/O error")]
    IoError(#[from] IoError),

    /// Error during building reqwest client
    #[error("failed to build reqwest client")]
    ReqwestClientBuildError(#[source] reqwest::Error),

    /// Error during downloading of the file
    #[error("download error")]
    DownloadError(#[from] reqwest::Error),

    /// Failed to wait for file to be downloaded
    ///
    /// This happens when one thread is waiting for
    /// a file, which is currently being downloaded by another thread,
    /// and that download fails.
    #[error("awaited file failed to download")]
    AwaitingError,

    #[error(transparent)]
    CleanUpError(Box<CleanUpError>),

    /// Occurs when attempted to perform some action on URL which is not cached.
    #[error("URL '{0}' is not cached")]
    UrlNotCached(String),

    /// Timeout exceeded
    #[error("timeout exceeded")]
    TimeoutExceeded,

    /// Custom error message
    #[error("{0}")]
    CustomError(String),

    #[error(transparent)]
    NonUtf8PathError(#[from] NonUtf8PathError),
}

/// Failed to properly clean-up cache after download failure.
///
/// Both errors `remove_file_error` and `database_error` happened at the same time.
///
/// If this is returned from [`Client::get`][crate::Client::get],
/// this most likely means that cache is not in an invalid state
/// and needs to be fixed manually. Because of that, this error should be
/// treated **as severe**.
#[derive(Debug)]
pub struct CleanUpError {
    /// Error happened during file removal
    pub remove_file_error: IoError,

    /// Error happened during cache entry removal
    pub database_error: DatabaseError,

    /// Original downloading error, which caused the clean-up
    pub download_error: Error,
}

impl fmt::Display for CleanUpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "failed to clean-up cache after download failed:\n\
        - removing corrupted file failed: {}\n\
        - removing cache entry failed: {}\n\
        This error probably means that your cache is now contains corrupted file.\n\
        Consider using Carol CLI to remove the entry manually.",
            eyre!("{:#}", &self.remove_file_error),
            eyre!("{:#}", &self.database_error)
        )
    }
}

impl std::error::Error for CleanUpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.download_error)
    }
}

/// Non UTF-8 symbol in path.
#[derive(thiserror::Error, Debug)]
#[error("non-UTF-8 symbol in path")]
pub struct NonUtf8PathError;

/// Maintenance operations error.
#[derive(thiserror::Error, Debug)]
pub enum MaintenanceError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("client error: {0}")]
    ClientError(#[from] Error),

    #[error("{0}")]
    NonUtf8PathError(#[from] NonUtf8PathError),
}
