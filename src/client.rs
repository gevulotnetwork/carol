use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Client as ReqwestClient;
use tokio::fs;
use tokio::io::{AsyncWriteExt, Error as IoError};
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{debug, trace};

use crate::database::models::CacheEntry;
use crate::database::{self, api, Connection};
use crate::errors::{CleanUpError, DatabaseError, Error, NonUtf8PathError};
use crate::{DateTime, File, FileStatus, RetryPolicy, Utc};

type StdResult<T, E> = std::result::Result<T, E>;
type Result<T> = StdResult<T, Error>;

/// Used to create precisely configured [`Client`].
#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
    cache_dir: PathBuf,
    database_url: String,
    reqwest_client: Option<ReqwestClient>,
    default_file_duration: Option<Duration>,
    download_retry_policy: RetryPolicy,
}

impl ClientBuilder {
    /// Create new client builder.
    pub fn new<P>(database_url: &str, cache_dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            database_url: database_url.to_string(),
            cache_dir: cache_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Set [`reqwest::Client`] to use for downloading.
    ///
    /// If not set, [`Client`] will build default [`reqwest::Client`] on initialization.
    pub fn reqwest_client(&mut self, reqwest_client: ReqwestClient) -> &mut Self {
        self.reqwest_client = Some(reqwest_client);
        self
    }

    /// Set default file duration.
    ///
    /// `duration` will be added to [`Utc::now()`] and set as an expiration timestamp when
    /// downloading a file. If not set, new files won't have any expiration timestamp and
    /// will never expire.
    pub fn default_file_duration(&mut self, duration: Duration) -> &mut Self {
        self.default_file_duration = Some(duration);
        self
    }

    /// Set retry policy for fetching new files.
    /// Applies only to downloading process during [`Client::get`].
    ///
    /// If not set, [`RetryPolicy::None`] will be used.
    pub fn download_retry_policy(&mut self, policy: RetryPolicy) -> &mut Self {
        self.download_retry_policy = policy;
        self
    }

    /// Build client.
    ///
    /// # Errors
    ///
    /// Returns error if client initialization fails.
    pub async fn build(self) -> Result<Client> {
        let mut client = Client::init(&self.database_url, &self.cache_dir).await?;
        if let Some(reqwest_client) = self.reqwest_client {
            client.reqwest_client = reqwest_client;
        }
        client.default_duration = self.default_file_duration;
        client.download_retry_policy = self.download_retry_policy;
        Ok(client)
    }
}

/// Carol client.
///
/// Operations like [`Client::get`] and [`Client::remove`] are performed in a transactional manner
/// and should not result in an invalid state.
/// If the state becomes broken anyway, maintenance could possibly detect corrupted state
/// and fix it.
///
/// Downloading is implemented through [`reqwest`].
///
/// See for [crate-level documentation][crate] for more information.
pub struct Client {
    /// Path to cache directory.
    pub(crate) cache_dir: PathBuf,

    /// Cache database connection.
    pub(crate) db: Connection,

    /// We store origin of the database to pass to `File`-s
    database_url: String,

    reqwest_client: ReqwestClient,
    default_duration: Option<Duration>,
    download_retry_policy: RetryPolicy,
}

impl Client {
    /// Create [`ClientBuilder`] for precise [`Client`] configuration.
    pub fn builder<P>(database_url: &str, cache_dir: P) -> ClientBuilder
    where
        P: AsRef<Path>,
    {
        ClientBuilder::new(database_url, cache_dir)
    }

    /// Initialize Carol client.
    ///
    /// Initializes cache directory and database if they don't exist.
    pub async fn init<P>(database_url: &str, cache_dir: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        trace!("running migrations on {}", database_url);
        database::run_migrations(database_url).await?;

        trace!("establishing cache database connection: {}", database_url);
        let db = database::establish_connection(database_url).await?;

        // Database must store absolute paths for cached files,
        // so this path will be stored as absolute.
        let cache_dir = std::path::absolute(cache_dir)?;
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir).await?;
        }

        let reqwest_client = ReqwestClient::builder()
            .build()
            .map_err(Error::ReqwestClientBuildError)?;

        Ok(Self {
            cache_dir,
            db,
            database_url: database_url.to_string(),
            default_duration: None,
            reqwest_client,
            download_retry_policy: RetryPolicy::None,
        })
    }

    /// Download the file from URL into cache.
    ///
    /// If the file is already cached, it won't be re-downloaded.
    pub async fn get<T>(&mut self, url: T) -> Result<File>
    where
        T: AsRef<str>,
    {
        let url = url.as_ref();
        debug!("get URL: {}", url);
        let url_hash = sha256::digest(url);
        let cache_path = self.cache_dir.join(&url_hash);

        let result = api::new_entry(
            &mut self.db,
            url,
            cache_path.to_str().ok_or(NonUtf8PathError)?,
            self.default_duration.map(|period| Utc::now() + period),
        )
        .await;

        match result {
            Ok(entry) => {
                // File was not in cache, so let's download it
                debug!("cache miss: {:?}", entry);
                match self.fetch_entry_with_retry(&entry).await {
                    Ok(entry) => {
                        // At this point file was downloaded and stored in cache
                        // If this function fails, it means that we only failed
                        // to increment the reference counter. So it is safe
                        // to just return an error here and let user call `get()` again.
                        self.create_locked_file(entry).await
                    }
                    Err(download_error) => match self.cleanup_entry(&entry).await {
                        Ok(_) => Err(download_error),
                        Err((remove_file_error, database_error)) => {
                            Err(Error::CleanUpError(Box::new(CleanUpError {
                                remove_file_error,
                                database_error,
                                download_error,
                            })))
                        }
                    },
                }
            }
            Err(err) if err.is_unique_violation() => {
                // File is already in cache
                debug!("cache hit: {}", url);
                let entry = self.wait_entry(url).await?;

                // At this point file was downloaded and stored in cache
                // If this function fails, it means that we only failed
                // to increment the reference counter. So it is safe
                // to just return an error here and let user call `get()` again.
                self.create_locked_file(entry).await
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Find file by URL in cache. Returns `None` is URL is not cached.
    pub async fn find<T>(&mut self, url: T) -> Result<Option<File>>
    where
        T: AsRef<str>,
    {
        debug!("lookup URL '{}'", url.as_ref());
        let maybe_entry = api::get_by_url(&mut self.db, url.as_ref()).await?;
        Ok(if let Some(entry) = maybe_entry {
            Some(self.create_locked_file(entry).await?)
        } else {
            None
        })
    }

    /// Set expiration timestamp for URL.
    ///
    /// Returns error if URL is not cached.
    pub async fn set_expires<T>(&mut self, url: T, expires_at: DateTime<Utc>) -> Result<()>
    where
        T: AsRef<str>,
    {
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            api::update_expires(&mut self.db, entry.id, Some(expires_at)).await?;
            Ok(())
        } else {
            Err(Error::UrlNotCached(url.as_ref().to_string()))
        }
    }

    /// Remove URL from cache.
    ///
    /// Does nothing if the URL is not cached.
    pub async fn remove<T>(&mut self, url: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        debug!("removing URL '{}' from cache", url.as_ref());
        // If this fails, nothing is wrong with the state.
        // File can be safely removed later.
        self.schedule_for_removal(&url).await?;

        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            // If this fails, the file will still be marked as `ToRemove`,
            // so it will be garbage collected later.
            api::remove_entry(&mut self.db, entry.id).await?;

            // If this fails, we will have a dangling file in cache directory,
            // which will be removed later on maintenance.
            fs::remove_file(entry.cache_path).await?;
        }
        Ok(())
    }

    /// Schedule URL for removal (set its status to [`FileStatus::ToRemove`]).
    ///
    /// Does nothing if the URL is not cached.
    pub async fn schedule_for_removal<T>(&mut self, url: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        debug!("scheduling URL '{}' for removal", url.as_ref());
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            api::update_status(&mut self.db, entry.id, FileStatus::ToRemove).await?;
        }
        Ok(())
    }

    /// Force-remove URL from cache.
    ///
    /// This is an unsafe method, which doesn't check reference counter of the file.
    /// Calling this may lead to file being removed while in use or
    /// cache corruption.
    ///
    /// # Safety
    ///
    /// User must ensure the file is not used and schedule it for removal first.
    pub async unsafe fn remove_unsafe<T>(&mut self, url: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            // Since this is unsafe method, calling it may lead to invalid cache state
            api::delete_unsafe(&mut self.db, entry.id).await?;
            fs::remove_file(entry.cache_path).await?;
        }
        Ok(())
    }

    /// List all cache entries.
    pub async fn list(&mut self) -> Result<Vec<File>> {
        let entries = api::get_all(&mut self.db)
            .await
            .map(|res| res.into_iter().collect::<Vec<_>>())?;
        let mut files = Vec::new();
        // TODO: iterate files asynchronously
        for entry in entries {
            files.push(self.create_locked_file(entry).await?);
        }
        Ok(files)
    }

    /// Update file with given URL.
    ///
    /// This will remove old cache entry and create new one.
    ///
    /// # Errors
    ///
    /// Returns error if:
    ///
    /// - URL is not cached
    /// - File is used at the moment
    pub async fn update<T>(&mut self, url: T) -> Result<File>
    where
        T: AsRef<str>,
    {
        debug!("updating URL '{}'", url.as_ref());
        if api::get_by_url(&mut self.db, url.as_ref()).await?.is_some() {
            self.remove(url.as_ref()).await?;
            self.get(url.as_ref()).await
        } else {
            Err(Error::UrlNotCached(url.as_ref().to_string()))
        }
    }

    /// Wait until file reference counter for URL becomes 0.
    ///
    /// Loops with period of 1 second until reach `timeout`.
    ///
    /// # Warning
    ///
    /// This method **does not guarantee** that right after return file cannot become again.
    /// E.g. remove right after this call may stil fail with "reference counter is not 0".
    /// This will work only in relaxed scenarios, when files are not frequently work.
    /// Basically used during maintenance.
    ///
    /// # Errors
    ///
    /// Returns error if:
    ///
    /// - URL is not cached
    /// - Timeout is exceeded
    pub async fn wait_url_released<T>(&mut self, url: T, timeout: Duration) -> Result<()>
    where
        T: AsRef<str>,
    {
        const PERIOD: Duration = Duration::from_secs(1);

        debug!("waiting URL '{}' to become free", url.as_ref());
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            let mut inner = async || -> Result<()> {
                loop {
                    let entry = api::get_entry(&mut self.db, entry.id).await?;
                    if entry.ref_count == 0 {
                        debug!("URL '{}' is free", url.as_ref());
                        break;
                    } else {
                        // file is still used, come back later
                        time::sleep(PERIOD).await;
                    }
                }
                Ok(())
            };

            match time::timeout(timeout, inner()).await {
                Ok(result) => result,
                Err(_) => Err(Error::TimeoutExceeded),
            }
        } else {
            Err(Error::UrlNotCached(url.as_ref().to_string()))
        }
    }

    /// Fetch entry with respect to donwloading retry policy.
    async fn fetch_entry_with_retry(&mut self, entry: &CacheEntry) -> Result<CacheEntry> {
        match self.fetch_entry(entry).await {
            Ok(entry) => Ok(entry),
            Err(err) => match self.download_retry_policy {
                RetryPolicy::None => Err(err),
                RetryPolicy::Fixed { number, period } => {
                    let mut result = Err(err);

                    // retry cycle
                    for i in 0..number {
                        time::sleep(period).await;
                        debug!("retrying fetch URL '{}', attempt #{}", entry.url, i + 1);
                        match self.fetch_entry(entry).await {
                            Ok(entry) => {
                                result = Ok(entry);
                                break;
                            }
                            Err(err) => result = Err(err),
                        }
                    }

                    // latest occured error will be thrown
                    result
                }
            },
        }
    }

    /// Query entry URL and store response body in target file.
    /// Then set entry status to `Ready`.
    async fn fetch_entry(&mut self, entry: &CacheEntry) -> Result<CacheEntry> {
        let mut file = fs::File::create(&entry.cache_path).await?;

        trace!("fetching {}", &entry.url);
        let response = self
            .reqwest_client
            .get(&entry.url)
            .send()
            .await?
            .error_for_status()?;
        let code = response.status().as_u16();
        if code != 200 {
            // For responses with 2XX codes we probably only interested in 200.
            return Err(Error::CustomError(format!(
                "HTTP response status code {}",
                code
            )));
        }
        let mut stream = response.bytes_stream();
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            file.write_all(&chunk).await?;
        }
        let updated_entry = api::update_status(&mut self.db, entry.id, FileStatus::Ready).await?;
        Ok(updated_entry)
    }

    /// Clean-up entry: remove file from cache directory and remove cache entry from database.
    ///
    /// Returns errors if both operations failed.
    async fn cleanup_entry(
        &mut self,
        entry: &CacheEntry,
    ) -> StdResult<(), (IoError, DatabaseError)> {
        let file_result = fs::remove_file(&entry.cache_path).await;
        let entry_result = unsafe { api::delete_unsafe(&mut self.db, entry.id) }.await;
        match (file_result, entry_result) {
            (Err(file_error), Err(entry_error)) => {
                // This is a severe clean-up error, which cannot be resolved automatically
                // Corrupted file is left in a cache, but Carol has no identification about
                // this entry being corrupt.
                Err((file_error, entry_error))
            }
            _ => Ok(()),
        }
    }

    /// Wait until file comes out of `Pending`.
    async fn wait_entry(&mut self, url: &str) -> Result<CacheEntry> {
        debug!("awaiting URL: {}", url);
        let maybe_id = api::get_by_url(&mut self.db, url)
            .await?
            .map(|entry| entry.id);

        if let Some(id) = maybe_id {
            loop {
                match api::get_entry(&mut self.db, id).await {
                    Ok(entry) if entry.status == FileStatus::Pending => {
                        // file is still downloading, come back later
                        time::sleep(Duration::from_secs(1)).await;
                    }
                    Ok(entry) => return Ok(entry),
                    Err(err) if err.is_not_found() => {
                        // If at some point entry dissapeared, it means that is was removed
                        // There are two reasons for that:
                        // - it was garbage collected (this is very unlikely to happened, so we will ignore this)
                        // - downloading failed
                        // So we will return downloading error here
                        return Err(Error::AwaitingError);
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        } else {
            Err(Error::UrlNotCached(url.to_string()))
        }
    }

    /// Pair database URL with cache entry, create [`File`] and increment its reference counter.
    async fn create_locked_file(&mut self, entry: CacheEntry) -> Result<File> {
        let mut file = File::from_entry(entry, self.database_url.clone());
        api::increment_ref(&mut self.db, file.id).await?;
        file.released = false;
        Ok(file)
    }
}

// TODO: Most of the method implementations abuse lock/release system of files,
//       which results into a lot of unneccessary database connections.
//       It would be nice to re-write implementations without the use of `File`-s.

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("cache_dir", &self.cache_dir)
            .field("database_url", &self.database_url)
            .field("default_duration", &self.default_duration)
            .field("reqwest_client", &self.reqwest_client)
            .finish()
    }
}

/// Client fixtures. Helps in testing client-related code.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) mod fixtures {
    use super::ReqwestClient;
    use crate::database::fixtures::{database, CacheDatabaseFixture};
    use crate::database::models::{CacheEntry, NewCacheEntry};
    use crate::{Client, RetryPolicy};
    use crate::{DateTime, FileStatus, Utc};
    use http_test_server::http::{Method, Status};
    use http_test_server::TestServer;
    use rstest::fixture;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::fs;
    use tracing::trace;

    pub const PORT: u16 = 44256;
    pub const DEFAULT_PATH: &str = "/file.txt";
    pub const DEFAULT_URL: &str = "http://localhost:44256/file.txt";
    pub const DEFAULT_CONTENT: &str = "This is test file.";

    /// Test HTTP server with a single resource on `http://localhost:44256/file.txt`.
    #[fixture]
    #[once]
    pub fn test_server() -> TestServer {
        let server = TestServer::new_with_port(PORT).unwrap();
        let resource = server.create_resource(DEFAULT_PATH);
        resource
            .status(Status::OK)
            .method(Method::GET)
            .header("Content-Type", "text/plain")
            .body(DEFAULT_CONTENT);
        server
    }

    /// Fixture for initialized cache.
    pub struct CacheFixture {
        /// Client with temp database and cache directory.
        pub client: Client,

        /// Hold cache directory and destroys it on drop.
        pub cache_dir: TempDir,

        /// Database fixture.
        pub db: CacheDatabaseFixture,
    }

    impl CacheFixture {
        pub async fn new(
            db: CacheDatabaseFixture,
            retry_policy: RetryPolicy,
            file_duration: Option<Duration>,
            reqwest_client: Option<ReqwestClient>,
        ) -> Self {
            trace!("creating CacheFixture");
            let cache_dir = tempfile::tempdir().unwrap();
            let mut client_builder = Client::builder(&db.db_path, cache_dir.path());
            if let Some(duration) = file_duration {
                client_builder.default_file_duration(duration);
            }
            if let Some(client) = reqwest_client {
                client_builder.reqwest_client(client);
            }
            client_builder.download_retry_policy(retry_policy);
            trace!(
                "building client in cache fixture from: {:?}",
                &client_builder
            );
            let client = client_builder.build().await.unwrap();
            CacheFixture {
                client,
                db,
                cache_dir,
            }
        }
    }

    /// New empty cache.
    #[fixture]
    #[awt]
    pub async fn cache(
        #[default(RetryPolicy::None)] retry_policy: RetryPolicy,
        #[default(None)] file_duration: Option<Duration>,
        #[default(None)] reqwest_client: Option<ReqwestClient>,
        #[future] database: CacheDatabaseFixture,
    ) -> CacheFixture {
        CacheFixture::new(database, retry_policy, file_duration, reqwest_client).await
    }

    pub type CacheWithFileFixture = (CacheFixture, CacheEntry);

    /// New cache with a signle file stored.
    #[fixture]
    #[awt]
    pub async fn cache_with_file(
        #[default(DEFAULT_URL)] url: impl AsRef<str>,
        #[default(FileStatus::Ready)] status: FileStatus,
        #[default(None)] expires: Option<DateTime<Utc>>,
        #[default(0)] ref_count: i32,
        // This default value is sha256 of default url
        #[default("1f8b6bd39e9e70cc634217b4686e25c715c82f3fe364c6454399e0aa60118ea4")]
        filename: impl AsRef<str>,
        #[default(DEFAULT_CONTENT)] content: impl AsRef<str>,
        #[default(RetryPolicy::None)] retry_policy: RetryPolicy,
        #[default(None)] file_duration: Option<Duration>,
        #[default(None)] reqwest_client: Option<ReqwestClient>,
        #[future] database: CacheDatabaseFixture,
    ) -> CacheWithFileFixture {
        trace!("IN cache_with_file");
        let mut cache =
            CacheFixture::new(database, retry_policy, file_duration, reqwest_client).await;
        let cache_path = cache
            .cache_dir
            .path()
            .join(filename.as_ref())
            .to_str()
            .unwrap()
            .to_string();
        fs::write(&cache_path, content.as_ref()).await.unwrap();
        let entry = cache
            .db
            .insert_entry(NewCacheEntry {
                url: url.as_ref().to_string(),
                cache_path,
                created: Utc::now(),
                expires,
                status,
                ref_count,
            })
            .await;
        (cache, entry)
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{
        cache, cache_with_file, test_server, CacheFixture, CacheWithFileFixture, DEFAULT_CONTENT,
        DEFAULT_URL,
    };
    use super::Client;
    use crate::database::api;
    use crate::{DateTime, FileStatus, RetryPolicy, Utc};
    use http_test_server::TestServer;
    use rstest::rstest;
    use std::time::Duration;
    use tokio::fs;
    use tracing::trace;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_init_new_cache() {
        let tmp_database = tempfile::tempdir().unwrap();
        let database = tmp_database.path().join("carol.sqlite");
        let database_path = database.as_os_str().to_str().unwrap().to_string();

        let tmp_cache_dir = tempfile::tempdir().unwrap();

        Client::init(&database_path, tmp_cache_dir.path())
            .await
            .expect("initialize new cache");
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_connect_to_existing_cache(#[future] cache: CacheFixture) {
        trace!("begin test");
        Client::init(&cache.db.db_path, &cache.cache_dir)
            .await
            .expect("connect to existing cache");
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get(
        #[future] mut cache: CacheFixture,
        #[allow(unused)] test_server: &TestServer,
    ) {
        trace!("begin test");
        let file = cache
            .client
            .get(DEFAULT_URL)
            .await
            .expect("get file from URL");
        assert_eq!(
            file.status().await.expect("get file status"),
            FileStatus::Ready
        );
        assert_eq!(file.url(), DEFAULT_URL);
        let content = fs::read_to_string(file.cache_path())
            .await
            .expect("read downloaded file");
        assert_eq!(content, DEFAULT_CONTENT);
        let entry = api::get_entry(&mut cache.db.conn, file.id).await.unwrap();
        assert_eq!(entry.ref_count, 1);
        file.release().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get_not_re_downloading(#[future] cache_with_file: CacheWithFileFixture) {
        trace!("begin test");
        let (mut cache, entry) = cache_with_file;
        let file = cache
            .client
            .get(DEFAULT_URL)
            .await
            .expect("get existing file from URL");
        assert_eq!(file.id, entry.id);
        let content = fs::read_to_string(file.cache_path())
            .await
            .expect("read existing file");
        assert_eq!(content, DEFAULT_CONTENT);
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_downloading_retry(
        #[future]
        #[with(RetryPolicy::Fixed { number: 3, period: Duration::from_secs(1) })]
        cache: CacheFixture,
        test_server: &TestServer,
    ) {
        trace!("begin test");
        let url = format!("http://localhost:{}/new_file.txt", test_server.port());

        // Create resource after 2500 millisecs
        // Give client time to fail 2 times before providing resource
        let create_resource = async || {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            use http_test_server::http::{Method, Status};

            let resource = test_server.create_resource("/new_file.txt");
            resource
                .status(Status::OK)
                .method(Method::GET)
                .header("Content-Type", "text/plain")
                .body("This is new test file.");
        };

        let get_resource = async || {
            let mut client = cache.client;
            let file = client
                .get(&url)
                .await
                .expect("get URL without errors after 3 retries");
            let content = fs::read_to_string(file.cache_path())
                .await
                .expect("read downloaded file");
            assert_eq!(content, "This is new test file.");
        };

        tokio::join!(create_resource(), get_resource());

        // check that client tried fetching 2 times before success
        assert!(logs_contain(&format!(
            "retrying fetch URL '{}', attempt #1",
            url
        )));
        assert!(logs_contain(&format!(
            "retrying fetch URL '{}', attempt #2",
            url
        )));
    }

    #[rstest]
    #[case::unused_file(0)]
    #[should_panic(expected = "remove URL from cache: DatabaseError(RemoveError(UsedFile))")]
    #[case::used_file(1)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_remove(
        #[allow(unused)]
        #[case]
        ref_count: i32,
        #[future]
        #[with(DEFAULT_URL, FileStatus::Ready, None, ref_count)]
        cache_with_file: CacheWithFileFixture,
    ) {
        trace!("begin test");
        let (cache, _) = cache_with_file;
        let mut client = cache.client;
        client
            .remove(DEFAULT_URL)
            .await
            .expect("remove URL from cache");
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_set_expires(#[future] cache_with_file: CacheWithFileFixture) {
        trace!("begin test");
        let (mut cache, entry) = cache_with_file;
        let mut client = cache.client;
        client
            .set_expires(DEFAULT_URL, DateTime::<Utc>::MAX_UTC)
            .await
            .expect("set expiration timestamp");
        let updated_entry = api::get_entry(&mut cache.db.conn, entry.id).await.unwrap();
        assert_eq!(updated_entry.expires, Some(DateTime::<Utc>::MAX_UTC));
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_schedule_for_removal(#[future] cache_with_file: CacheWithFileFixture) {
        trace!("begin test");
        let (mut cache, entry) = cache_with_file;
        let mut client = cache.client;
        client
            .schedule_for_removal(DEFAULT_URL)
            .await
            .expect("schedule URL for removal");
        let updated_entry = api::get_entry(&mut cache.db.conn, entry.id).await.unwrap();
        assert_eq!(updated_entry.status, FileStatus::ToRemove);
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_update(
        #[future] cache_with_file: CacheWithFileFixture,
        #[allow(unused)] test_server: &TestServer,
    ) {
        trace!("begin test");
        let (mut cache, entry) = cache_with_file;
        let mut client = cache.client;
        client
            .update(&DEFAULT_URL)
            .await
            .expect("update cache entry");
        let updated_entry = api::get_entry(&mut cache.db.conn, entry.id).await.unwrap();
        assert!(updated_entry.created > entry.created);
    }

    #[rstest]
    #[case::wait_enough_time(Duration::from_secs(5))]
    #[should_panic(expected = "wait for URL released: TimeoutExceeded")]
    #[case::wait_not_enough_time(Duration::from_secs(1))]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_wait_url_released(
        #[with(DEFAULT_URL, FileStatus::Ready, None, 1)]
        #[future]
        cache_with_file: CacheWithFileFixture,
        #[case] wait_time: Duration,
    ) {
        trace!("begin test");
        let (mut cache, entry) = cache_with_file;
        let mut client = cache.client;

        // Release file after 3 seconds
        let mut release_file = async || {
            tokio::time::sleep(Duration::from_secs(3)).await;
            api::decrement_ref(&mut cache.db.conn, entry.id)
                .await
                .unwrap();
        };

        // Wait 5 seconds for URL to become free
        let mut wait_for_url_released = async || {
            client
                .wait_url_released(DEFAULT_URL, wait_time)
                .await
                .expect("wait for URL released");
        };

        tokio::join!(release_file(), wait_for_url_released());
    }
}
