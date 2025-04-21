//! # Carol files storage
//!
//! This library implements an asynchronous managed storage of files in the filesystem. Storage
//! files directory is paired with a database, which holds files additional metadata. It works with
//! any filesystem supported by Rust and currently backed by serverless SQLite database.
//!
//! ## How it works
//!
//! Carol storage consists of two parts:
//!
//! - **storage directory** in your filesystem, where the files are stored;
//! - **storage database** which keeps metadata about stored files and provides synchronization on
//!   their updates.
//!
//! To interact with storage use [`StorageManager`] instance. Before initializing a manager, create
//! storage directory first. Manager **will not** do this.
//!
//! ## Example
//!
//! ```rust
//! # tokio_test::block_on(async {
//! use carol::{FileSource, StorageManager, StorePolicy};
//!
//! let cache_dir = "/tmp/carol";
//!
//! // Create storage directory
//! std::fs::create_dir_all(&cache_dir).unwrap();
//!
//! // Initialize storage manager
//! let manager = StorageManager::init("carol.sqlite", &cache_dir, None).await.unwrap();
//!
//! // Copy some local file into storage
//! std::fs::write("myfile", "hello world").unwrap();
//!
//! let file = manager
//!     .copy_local_file(
//!         FileSource::parse("./myfile"),
//!         StorePolicy::StoreForever,
//!         Some("myfile".to_string()),
//!         "myfile",
//!     )
//!     .await
//!     .unwrap();
//! # std::fs::remove_dir_all(&cache_dir).unwrap();
//! # std::fs::remove_file("myfile").unwrap();
//! # drop(manager);
//! # std::fs::remove_file("carol.sqlite").unwrap();
//! # })
//! ```

mod database;
mod error;
mod file;
mod storage_config;
mod storage_manager;

// Public modules
pub mod sqlite;

// Public re-exports
pub use database::{StorageDatabase, StorageDatabaseError, StorageDatabaseExt};
pub use error::{NonUtf8PathError, StorageError};
pub use file::{
    File, FileId, FileLock, FileLockError, FileLockMode, FileMetadata, FileSource, FileStatus,
    StorePolicy,
};
pub use storage_config::{EvictionPolicy, StorageConfig};
pub use storage_manager::StorageManager;

// Re-exports of extern crates containing types referenced in public API
#[doc(no_inline)]
pub use chrono;
#[doc(no_inline)]
pub use url;
