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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::client::fixtures::ClientFixture;
//     use crate::{DateTime, Utc};
//     use tracing_test::traced_test;

//     #[tokio::test]
//     #[traced_test]
//     async fn test_schedule_for_removal() {
//         let fixture = ClientFixture::new().await.unwrap();
//         let mut client = fixture.client;

//         let url = format!("{}/file.txt", &fixture.host);
//         client.get(&url).await.unwrap();
//         client
//             .set_expires(&url, DateTime::<Utc>::MIN_UTC)
//             .await
//             .unwrap();

//         let mut cleaner = CacheCleaner::new(&mut client);
//         cleaner
//             .schedule_for_removal()
//             .await
//             .expect("schedule for removal");

//         let status = client.get(&url).await.unwrap().status().await.unwrap();
//         assert_eq!(status, FileStatus::ToRemove);
//     }

//     #[tokio::test]
//     #[traced_test]
//     async fn test_remove() {
//         let fixture = ClientFixture::new().await.unwrap();
//         let mut client = fixture.client;

//         let url = format!("{}/file.txt", &fixture.host);
//         let file = client.get(&url).await.unwrap();
//         file.release().await.unwrap();
//         client.schedule_for_removal(&url).await.unwrap();

//         let mut cleaner = CacheCleaner::new(&mut client);
//         cleaner
//             .remove()
//             .await
//             .expect("remove files with 'ToRemove' status");

//         let maybe_file = client.find(&url).await.unwrap();
//         assert!(maybe_file.is_none());
//     }

//     #[tokio::test]
//     #[traced_test]
//     async fn test_run_once() {
//         let fixture = ClientFixture::new().await.unwrap();
//         let mut client = fixture.client;

//         let url = format!("{}/file.txt", &fixture.host);
//         let file = client.get(&url).await.unwrap();
//         file.release().await.unwrap();
//         client
//             .set_expires(&url, DateTime::<Utc>::MIN_UTC)
//             .await
//             .unwrap();

//         let mut cleaner = CacheCleaner::new(&mut client);
//         cleaner.run_once().await.expect("run cache cleaner");

//         let maybe_file = client.find(&url).await.unwrap();
//         assert!(maybe_file.is_none());
//     }
// }
