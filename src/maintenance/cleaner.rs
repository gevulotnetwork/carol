use chrono::Utc;
use tracing::{error, info};

use crate::database::api;
use crate::errors::Error;
use crate::{Client, FileStatus};

/// Cache cleaner.
///
/// Removed expired files from cache.
pub struct CacheCleaner<'c> {
    client: &'c mut Client,
}

impl<'c> CacheCleaner<'c> {
    /// Create new cache cleaner instance.
    pub fn new(client: &'c mut Client) -> Self {
        Self { client }
    }

    /// Run cache cleaning once.
    pub async fn run_once(&mut self) -> Result<(), Error> {
        info!("running cache cleaner");
        self.schedule_for_removal().await?;
        self.remove().await?;
        Ok(())
    }

    /// Set expired files status to [`FileStatus::ToRemove`].
    pub async fn schedule_for_removal(&mut self) -> Result<(), Error> {
        info!("scheduling expired files for removal");
        let now = Utc::now();
        let files = self.client.list().await?; // FIXME: any iterators in diesel?

        let mut to_remove = vec![];
        for file in files.into_iter() {
            if let Some(ts) = file.expires().await? {
                if ts < now {
                    to_remove.push(file);
                }
            }
        }
        info!("expired files: {}", to_remove.len());

        for file in to_remove {
            match self.client.schedule_for_removal(file.url()).await {
                Ok(_) => {
                    info!("URL '{}' scheduled for removal", file.url());
                }
                Err(err) => {
                    error!(
                        "failed to schedule URL '{}' for removal: {:?}",
                        file.url(),
                        err
                    );
                }
            }
        }
        Ok(())
    }

    /// Remove files with status `ToRemove`.
    pub async fn remove(&mut self) -> Result<(), Error> {
        info!("removing files with 'ToRemove' status");
        let to_remove = api::filter_by_status(&mut self.client.db, FileStatus::ToRemove).await?;
        info!("files to remove: {}", to_remove.len());
        for entry in to_remove {
            match self.client.remove(&entry.url).await {
                Ok(_) => {
                    info!("successfully removed URL '{}'", &entry.url);
                }
                Err(err) => {
                    // This may happened if the file was used after being scheduled for removal
                    // Its expiration timestamp may also have been updated
                    error!("failed to remove URL '{}': {:?}", entry.url, err);
                }
            }
        }
        Ok(())
    }
}

// TODO: Garbage collector may run a pool of clients to perform removals asynchronously.
//       For now files are removed sequentially, because &mut Client cannot be shared.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::fixtures::{cache_with_file, CacheWithFileFixture, DEFAULT_URL};
    use crate::database::api;
    use crate::{DateTime, Utc};
    use rstest::rstest;
    use tracing::trace;
    use tracing_test::traced_test;

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_schedule_for_removal(
        #[future]
        #[with(DEFAULT_URL, FileStatus::Ready, Some(DateTime::<Utc>::MIN_UTC))]
        cache_with_file: CacheWithFileFixture,
    ) {
        trace!("begin test");
        let (mut cache, entry) = cache_with_file;
        let mut client = cache.client;

        let mut cleaner = CacheCleaner::new(&mut client);
        cleaner
            .schedule_for_removal()
            .await
            .expect("schedule for removal all expired files");

        let entry = api::get_entry(&mut cache.db.conn, entry.id).await.unwrap();
        assert_eq!(entry.status, FileStatus::ToRemove);
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_remove(
        #[future]
        #[with(DEFAULT_URL, FileStatus::ToRemove)]
        cache_with_file: CacheWithFileFixture,
    ) {
        trace!("begin test");
        let (mut cache, _) = cache_with_file;
        let mut client = cache.client;

        let mut cleaner = CacheCleaner::new(&mut client);
        cleaner
            .remove()
            .await
            .expect("remove files with 'ToRemove' status");

        let entries = api::get_all(&mut cache.db.conn).await.unwrap();
        assert!(entries.is_empty());
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_run_once(
        #[future]
        #[with(DEFAULT_URL, FileStatus::Ready, Some(DateTime::<Utc>::MIN_UTC))]
        cache_with_file: CacheWithFileFixture,
    ) {
        trace!("begin test");
        let (mut cache, _) = cache_with_file;
        let mut client = cache.client;

        let mut cleaner = CacheCleaner::new(&mut client);
        cleaner.run_once().await.expect("run cache cleaner");

        let entries = api::get_all(&mut cache.db.conn).await.unwrap();
        assert!(entries.is_empty());
    }
}
