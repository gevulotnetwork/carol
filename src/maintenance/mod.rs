//! Maintenance operations on cache.
//!
//! Maintenance provides options to automatically fix different issues in cache.
//! These issues are not critical and the state of the cache is still valid,
//! however such situations are usually undesired.
//!
//! E.g. if during remove of the file cache entry was removed, but the file for
//! some reason was left in cache directory, [`MaintenanceRunner`] can automactically
//! remove it.
//!
//! # Example
//!
//! ```rust
//! # use carol::Client;
//! # async fn test(client: &mut Client) {
//! use carol::maintenance::{MaintenanceOpts, MaintenanceRunner};
//!
//! // Configure maintenance
//! let opts = MaintenanceOpts::builder()
//!     .run_cache_cleaning(true)
//!     .find_corrupted(true)
//!     .remove_corrupted(true)
//!     .prune_dangling(true)
//!     .build()
//!     .unwrap();
//!
//! // Create runned
//! let mut runner = MaintenanceRunner::new(client, opts);
//!
//! // Run all configured steps
//! runner.run_once().await.unwrap();
//! # }
//! ```

use std::path::Path;

use derive_builder::Builder;
use tokio::fs::{self, DirEntry};
use tracing::{debug, error, info, warn};

use crate::database::api;
use crate::errors::{Error, MaintenanceError, NonUtf8PathError};
use crate::{Client, File, FileStatus};

mod cleaner;

pub use cleaner::CacheCleaner;

/// Maintenance options. Use [`MaintenanceOptsBuilder`] to create.
#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
#[builder(default)]
pub struct MaintenanceOpts {
    /// Run cache cleaning.
    ///
    /// Find expired cache entries and remove them.
    /// Enables step [`MaintenanceRunner::run_cache_cleaning`].
    run_cache_cleaning: bool,

    /// Find corrupted cache entries.
    ///
    /// Search for cache entries with cache entry path which does not exist.
    /// Enables step [`MaintenanceRunner::find_corrupted_cache_entries`].
    find_corrupted: bool,

    /// Remove corrupted cache entries.
    ///
    /// Search through database and remove files with [`FileStatus::Corrupted`] status.
    /// Enables step [`MaintenanceRunner::remove_corrupted_entries`]
    /// which will be executed after [`MaintenanceRunner::find_corrupted_cache_entries`]
    /// if it is also enabled.
    remove_corrupted: bool,

    /// Remove dangling files from cache directory.
    ///
    /// Search for files which are no longer in the cache database
    /// and remove them from cache directory.
    /// Enables step [`MaintenanceRunner::prune_dangling_files`].
    prune_dangling: bool,
}

impl MaintenanceOpts {
    /// Create new builder for maintenance options.
    pub fn builder() -> MaintenanceOptsBuilder {
        MaintenanceOptsBuilder::default()
    }
}

/// Maintenance operations for Carol cache.
///
/// This runner is used to ensure Carol state is valid during runtime.
/// It tries to fix problems with cache directory and database if some.
/// If runner is not able to solve the problem, it will produce a warning in logs.
pub struct MaintenanceRunner<'c> {
    client: &'c mut Client,
    opts: MaintenanceOpts,
}

impl<'c> MaintenanceRunner<'c> {
    /// Create new maintenance runner.
    pub fn new(client: &'c mut Client, opts: MaintenanceOpts) -> Self {
        Self { client, opts }
    }

    /// Run all configured maintenance steps once.
    pub async fn run_once(&mut self) -> Result<(), MaintenanceError> {
        if self.opts.find_corrupted {
            self.find_corrupted_cache_entries().await?;
        }
        if self.opts.remove_corrupted {
            self.remove_corrupted_entries().await?;
        }
        if self.opts.run_cache_cleaning {
            self.run_cache_cleaning().await?;
        }
        if self.opts.prune_dangling {
            self.prune_dangling_files().await?;
        }
        Ok(())
    }

    /// Run cache cleaning: remove all expired cache entries.
    pub async fn run_cache_cleaning(&mut self) -> Result<(), MaintenanceError> {
        let mut gc = CacheCleaner::new(self.client);
        gc.run_once().await?;
        Ok(())
    }

    /// Find corrupted cache entries from database.
    ///
    /// Search for cache entries with cache entry path which does not exist
    /// and mark them as [`FileStatus::Corrupted`].
    pub async fn find_corrupted_cache_entries(&mut self) -> Result<(), MaintenanceError> {
        let files = self.client.list().await?;
        for file in files {
            if let Err(err) = self.check_file(&file).await {
                error!("failed to check file (URL '{}'): {}", file.url(), err);
            }
        }
        Ok(())
    }

    /// Search through database and remove files with [`FileStatus::Corrupted`] status.
    ///
    /// If running [`Self::find_corrupted_cache_entries`], this function should be called after.
    pub async fn remove_corrupted_entries(&mut self) -> Result<(), MaintenanceError> {
        info!("removing corrupted entries");
        let corrupted = api::filter_by_status(&mut self.client.db, FileStatus::Corrupted)
            .await
            .map_err(Error::from)?;
        info!("corrupted entries: {}", corrupted.len());
        for entry in corrupted {
            if let Err(err) = self.client.schedule_for_removal(&entry.url).await {
                error!(
                    "failed to schedule corrupted entry for removal (URL '{}'): {:?}",
                    &entry.url, err
                );
                continue;
            }

            if let Err(err) = api::remove_entry(&mut self.client.db, entry.id).await {
                error!(
                    "failed to remove corrupted cache entry (URL '{}'): {:?}",
                    &entry.url, err
                );
            }

            if Path::new(&entry.cache_path).exists() {
                if let Err(err) = fs::remove_file(&entry.cache_path).await {
                    error!(
                        "failed to remove corrupted file (URL '{}'): {:?}",
                        &entry.url, err
                    );
                }
            }
        }
        Ok(())
    }

    /// Prune dangling files from cache directory.
    ///
    /// Search for files which are no longer in the cache database
    /// and remove them from cache directory.
    pub async fn prune_dangling_files(&mut self) -> Result<(), MaintenanceError> {
        info!("pruning dangling files");
        debug!(
            "reading entries in cache directory: {}",
            self.client.cache_dir.display()
        );
        let mut entries = fs::read_dir(&self.client.cache_dir).await?;

        // TODO: can we check entries asynchronously with a single client?
        while let Some(entry) = entries.next_entry().await? {
            if let Err(err) = self.check_dir_entry(&entry).await {
                error!(
                    "failed to handle entry '{}': {}",
                    entry.path().display(),
                    err
                );
            }
        }

        Ok(())
    }

    /// Check if cached file is corrupted and set its status to [`FileStatus::Corrupted`] if it is.
    ///
    /// This includes:
    /// - checking that `file.cache_path` exists
    async fn check_file(&mut self, file: &File) -> Result<(), MaintenanceError> {
        if !file.cache_path().exists() {
            api::update_status(&mut self.client.db, file.id, FileStatus::Corrupted)
                .await
                .map_err(Error::from)?;
        }
        Ok(())
    }

    /// Check directory entry for presence in cache database and remove if it's not found.
    async fn check_dir_entry(&mut self, entry: &DirEntry) -> Result<(), MaintenanceError> {
        let path = entry.path();
        let metadata = entry.metadata().await?;

        // Ideally nothing apart regular files should be in cache directory,
        // however for now we are not going to clean this up.
        // So just emitting warning and proceeding to next entry.
        if !metadata.is_file() {
            warn!("entry in cache directory is not a file: {}", path.display());
            return Ok(());
        }

        if api::get_by_cache_path(
            &mut self.client.db,
            path.as_os_str().to_str().ok_or(NonUtf8PathError)?,
        )
        .await
        .map_err(Error::from)?
        .is_none()
        {
            info!(
                "file '{}' is not found in database - removing it",
                path.display()
            );
            fs::remove_file(&path).await?;
            info!("'{}' removed", path.display());
        }

        Ok(())
    }
}

// TODO: Asynchronously handle files in maintenance runner.
//       Right now we synchronously iterate over files, which is not efficient.

// #[cfg(test)]
// mod fixtures {
//     use super::{MaintenanceOpts, MaintenanceRunner};
//     use crate::client::fixtures::ClientFixture;
//     use crate::database::api;
//     use crate::FileStatus;
//     use tokio::fs;

//     type Error = Box<dyn std::error::Error>;

//     pub struct CarolFixture {
//         pub client: ClientFixture,
//     }

//     impl CarolFixture {
//         pub async fn new() -> Result<Self, Error> {
//             Ok(Self {
//                 client: ClientFixture::new().await?,
//             })
//         }

//         pub fn runner(&mut self) -> MaintenanceRunner {
//             MaintenanceRunner::new(
//                 &mut self.client.client,
//                 MaintenanceOpts::builder().build().unwrap(),
//             )
//         }

//         pub async fn with_entry(mut self) -> Result<Self, Error> {
//             let url = format!("{}/file.txt", &self.client.host);
//             let file = self.client.client.get(&url).await?;
//             file.release().await?;
//             Ok(self)
//         }

//         pub async fn remove_file(mut self) -> Result<Self, Error> {
//             self = self.with_entry().await?;
//             let url = format!("{}/file.txt", &self.client.host);
//             let file = self.client.client.find(&url).await?.unwrap();
//             fs::remove_file(file.cache_path()).await?;
//             file.release().await?;
//             Ok(self)
//         }

//         pub async fn with_corrupted_entry(mut self) -> Result<Self, Error> {
//             self = self.with_entry().await?;
//             let url = format!("{}/file.txt", &self.client.host);
//             let pk = api::get_by_url(&mut self.client.client.db, &url)
//                 .await?
//                 .unwrap()
//                 .id;
//             api::update_status(&mut self.client.client.db, pk, FileStatus::Corrupted).await?;
//             Ok(self)
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::fixtures::CarolFixture;
//     use crate::FileStatus;
//     use tracing_test::traced_test;

//     #[tokio::test]
//     #[traced_test]
//     async fn test_find_corrupted_cache_entries() {
//         let mut fixture = CarolFixture::new()
//             .await
//             .unwrap()
//             .remove_file()
//             .await
//             .unwrap();
//         let mut runner = fixture.runner();

//         runner
//             .find_corrupted_cache_entries()
//             .await
//             .expect("find corrupted cache entries");

//         let url = format!("{}/file.txt", &fixture.client.host);
//         let file = fixture.client.client.get(&url).await.unwrap();
//         assert_eq!(file.status().await.unwrap(), FileStatus::Corrupted);
//     }

//     #[tokio::test]
//     #[traced_test]
//     async fn test_remove_corrupted_cache_entries() {
//         let mut fixture = CarolFixture::new()
//             .await
//             .unwrap()
//             .with_corrupted_entry()
//             .await
//             .unwrap();
//         let mut runner = fixture.runner();

//         runner
//             .remove_corrupted_entries()
//             .await
//             .expect("remove corrupted cache entries");

//         let url = format!("{}/file.txt", &fixture.client.host);
//         let file = fixture.client.client.find(&url).await.unwrap();
//         assert!(file.is_none());
//     }
// }
