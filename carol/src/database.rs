use async_trait::async_trait;

use crate::file::{File, FileId, FileMetadata, FileSource, FileStatus};

pub trait StorageDatabaseError: std::error::Error + Send + Sync {
    fn is_unique_violation(&self) -> bool;
    fn is_not_found(&self) -> bool;
}

#[async_trait]
pub trait StorageDatabase: Clone + Sized + Send + Sync {
    /// Type of the URI used to address the database.
    type Uri: ToString;

    /// Error type of the database operations.
    type Error: StorageDatabaseError;

    fn uri(&self) -> Self::Uri;

    /// Put new file into the database.
    async fn store(&self, metadata: FileMetadata) -> Result<FileId, Self::Error>;

    /// Get file from the database.
    async fn get(&self, id: FileId) -> Result<File, Self::Error>;

    /// Remove file from database.
    async fn remove(&self, id: FileId) -> Result<(), Self::Error>;
}

// TODO: this trait doesn't look good, probably needs refactoring
#[async_trait]
pub trait StorageDatabaseExt: StorageDatabase {
    /// Get all files from database.
    async fn select_all(&self) -> Result<Vec<File>, Self::Error>;

    /// Get files matching given source from database.
    async fn select_by_source(&self, source: &FileSource) -> Result<Vec<File>, Self::Error>;

    /// Update status of a file with given ID.
    async fn update_status(&self, id: FileId, new_status: FileStatus) -> Result<File, Self::Error>;

    /// Get all files ordered by "created" timestamp. The earliest comes first.
    async fn order_by_created(&self) -> Result<Vec<File>, Self::Error>;

    /// Get all files ordered by "last used" timestamp. The earliest comes first.
    async fn order_by_last_used(&self) -> Result<Vec<File>, Self::Error>;
}

#[cfg(test)]
pub mod mocks {
    use super::*;
    use mockall::mock;

    mock! {
        #[derive(Debug)]
        pub StorageDatabaseError {}

        impl std::fmt::Display for StorageDatabaseError {
            #[concretize]
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error>;
        }

        impl std::error::Error for StorageDatabaseError {}

        impl StorageDatabaseError for StorageDatabaseError {
            fn is_unique_violation(&self) -> bool;
            fn is_not_found(&self) -> bool;
        }
    }

    mock! {
        pub StorageDatabase {}

        impl Clone for StorageDatabase {
            fn clone(&self) -> Self;
        }

        #[async_trait]
        impl StorageDatabase for StorageDatabase {
            type Uri=String;
            type Error=MockStorageDatabaseError;

            fn uri(&self) -> String;
            async fn store(&self, metadata: FileMetadata) -> Result<FileId, MockStorageDatabaseError>;
            async fn get(&self, id: FileId) -> Result<File, MockStorageDatabaseError>;
            async fn remove(&self, id: FileId) -> Result<(), MockStorageDatabaseError>;
        }
    }

    mock! {
        pub StorageDatabaseExt {}

        impl Clone for StorageDatabaseExt {
            fn clone(&self) -> Self;
        }

        #[async_trait]
        impl StorageDatabase for StorageDatabaseExt {
            type Uri=String;
            type Error=MockStorageDatabaseError;

            fn uri(&self) -> String;
            async fn store(&self, metadata: FileMetadata) -> Result<FileId, MockStorageDatabaseError>;
            async fn get(&self, id: FileId) -> Result<File, MockStorageDatabaseError>;
            async fn remove(&self, id: FileId) -> Result<(), MockStorageDatabaseError>;
        }

        #[async_trait]
        impl StorageDatabaseExt for StorageDatabaseExt {
            async fn select_all(&self) -> Result<Vec<File>, MockStorageDatabaseError>;
            async fn select_by_source(&self, source: &FileSource) -> Result<Vec<File>, MockStorageDatabaseError>;
            async fn update_status(&self, id: FileId, new_status: FileStatus) -> Result<File, MockStorageDatabaseError>;
            async fn order_by_created(&self) -> Result<Vec<File>, MockStorageDatabaseError>;
            async fn order_by_last_used(&self) -> Result<Vec<File>, MockStorageDatabaseError>;
        }
    }
}
