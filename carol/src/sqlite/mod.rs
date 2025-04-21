//! Implementation of storage database backed by SQLite.
//!
//! An SQLite database build with migrations from `./migrations`.

use std::fmt;

use async_trait::async_trait;
use diesel::{ConnectionError, ConnectionResult, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::time::{self, Duration};
use tracing::trace;

use crate::database::{StorageDatabase, StorageDatabaseExt};
use crate::file::{File, FileId, FileMetadata, FileSource, FileStatus};

mod api;
mod models;
mod schema;

pub mod error;

use error::{ConvertStorePolicyError, DatabaseError};

/// Inner SQLite connection type.
type Connection = SyncConnectionWrapper<SqliteConnection>;

/// SQLite pool connection manager type.
type ConnectionManager = AsyncDieselConnectionManager<Connection>;

/// Result of database operations.
pub type DatabaseResult<T> = Result<T, DatabaseError>;

/// Primary key type.
type PrimaryKey = i32;

const MIGRATIONS: EmbeddedMigrations =
    diesel_migrations::embed_migrations!("src/sqlite/migrations");

const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

const RETRY_TIMES: usize = 3;
const RETRY_PERIOD: Duration = Duration::from_millis(100);

/// Establish connection with SQLite database and configure it with:
///
/// - `PRAGMA journal_mode = WAL`
/// - `PRAGMA synchronous = NORMAL`
/// - `PRAGMA busy_timeout = 10_000`
///
/// We really want this to succeed, that's why we retry.
fn establish_connection(database_url: &str) -> BoxFuture<ConnectionResult<Connection>> {
    let fut = async move {
        trace!("establishing connection with {}", database_url);
        let mut connection = Connection::establish(database_url).await?;

        let query = format!(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = {};",
            BUSY_TIMEOUT.as_millis()
        );
        trace!("executing: {}", &query);
        let result = connection.batch_execute(&query).await;

        match result {
            Ok(_) => Ok(connection),
            Err(err) => {
                trace!("SQLite connection configuration failed: {:?}", err);
                let mut result = Err(ConnectionError::CouldntSetupConfiguration(err));
                for i in 0..RETRY_TIMES {
                    trace!("retrying, attempt #{}", i + 1);
                    time::sleep(RETRY_PERIOD).await;
                    if let Err(err) = connection.batch_execute(&query).await {
                        // Last occurred error will be returned
                        trace!("SQLite connection configuration failed: {:?}", err);
                        result = Err(ConnectionError::CouldntSetupConfiguration(err));
                    } else {
                        result = Ok(connection);
                        break;
                    }
                }
                result
            }
        }
    };
    fut.boxed()
}

/// Run pending migrations on SQLite database specified with `database_url`.
pub async fn run_migrations(database_url: &str) -> DatabaseResult<()> {
    let connection = establish_connection(database_url).await?;
    let mut async_wrapper: AsyncConnectionWrapper<Connection> =
        AsyncConnectionWrapper::from(connection);

    trace!("running pending migrations");
    tokio::task::spawn_blocking(move || {
        let applied = async_wrapper
            .run_pending_migrations(MIGRATIONS)
            .expect("run migrations");
        if applied.is_empty() {
            trace!("no migrations applied");
        } else {
            trace!("migrations applied:");
            for migration in &applied {
                trace!(" - {}", migration);
            }
        }
    })
    .await
    .map_err(|e| DatabaseError::MigrationError(e.to_string()))
}

/// Storage database backed by SQLite.
#[derive(Clone)]
pub struct SqliteStorageDatabase {
    database_url: String,
    pool: Pool<Connection>,
}

impl SqliteStorageDatabase {
    /// Build connection pool for SQLite database with `database_url`.
    ///
    /// For each established connection following configs will be applied:
    /// - `PRAGMA journal_mode = WAL`
    /// - `PRAGMA synchronous = NORMAL`
    /// - `PRAGMA busy_timeout = 10_000`
    ///
    /// If SQLite database does not exists, it will be created.
    ///
    /// `max_size` defines the maximum size of the pool and defaults to `cpu_count * 4`.
    pub async fn connect_pool(database_url: &str, max_size: Option<usize>) -> DatabaseResult<Self> {
        let mut config = ManagerConfig::default();
        config.custom_setup = Box::new(establish_connection);
        let manager = ConnectionManager::new_with_config(database_url, config);
        let mut pool_builder = Pool::builder(manager);
        if let Some(max_size) = max_size {
            pool_builder = pool_builder.max_size(max_size);
        }
        let pool = pool_builder.build()?;
        Ok(Self {
            pool,
            database_url: database_url.to_string(),
        })
    }

    pub fn model_to_file(&self, model: models::File) -> Result<File, ConvertStorePolicyError> {
        Ok(File {
            database: self.database_url.clone(),
            status: model.status.into(),
            id: model.id.into(),
            metadata: FileMetadata::try_from(model)?,
        })
    }
}

impl fmt::Debug for SqliteStorageDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStorageDatabase")
            .field("database_url", &self.database_url)
            .finish()
    }
}

#[async_trait]
impl StorageDatabase for SqliteStorageDatabase {
    type Uri = String;

    type Error = DatabaseError;

    fn uri(&self) -> String {
        self.database_url.clone()
    }

    async fn store(&self, metadata: FileMetadata) -> DatabaseResult<FileId> {
        let mut conn = self.pool.get().await?;
        let file = api::insert(conn.as_mut(), models::NewFile::try_from(metadata)?).await?;
        Ok(file.id.into())
    }

    async fn get(&self, id: FileId) -> DatabaseResult<File> {
        let mut conn = self.pool.get().await?;
        let file = api::get(conn.as_mut(), id.into()).await?;
        Ok(self.model_to_file(file)?)
    }

    async fn remove(&self, id: FileId) -> DatabaseResult<()> {
        let mut conn = self.pool.get().await?;
        api::delete(conn.as_mut(), id.into()).await
    }
}

#[async_trait]
impl StorageDatabaseExt for SqliteStorageDatabase {
    async fn select_all(&self) -> DatabaseResult<Vec<File>> {
        let mut conn = self.pool.get().await?;
        let files = api::get_all(conn.as_mut()).await?;
        Ok(files
            .into_iter()
            .map(|file| self.model_to_file(file))
            .collect::<Result<_, _>>()?)
    }

    async fn select_by_source(&self, source: &FileSource) -> DatabaseResult<Vec<File>> {
        let mut conn = self.pool.get().await?;
        let files = api::get_by_source(conn.as_mut(), source.as_str()).await?;
        Ok(files
            .into_iter()
            .map(|file| self.model_to_file(file))
            .collect::<Result<_, _>>()?)
    }

    async fn update_status(&self, id: FileId, new_status: FileStatus) -> DatabaseResult<File> {
        let mut conn = self.pool.get().await?;
        let file = api::update_status(conn.as_mut(), id.into(), new_status.into()).await?;
        Ok(self.model_to_file(file)?)
    }

    async fn order_by_created(&self) -> DatabaseResult<Vec<File>> {
        let mut conn = self.pool.get().await?;
        let files = api::order_by_created(conn.as_mut()).await?;
        Ok(files
            .into_iter()
            .map(|file| self.model_to_file(file))
            .collect::<Result<_, _>>()?)
    }

    async fn order_by_last_used(&self) -> DatabaseResult<Vec<File>> {
        let mut conn = self.pool.get().await?;
        let files = api::order_by_last_used(conn.as_mut()).await?;
        Ok(files
            .into_iter()
            .map(|file| self.model_to_file(file))
            .collect::<Result<_, _>>()?)
    }
}

/// Database [`rstest`] fixtures. Helps in testing database-related code.
#[cfg(test)]
pub mod fixtures {
    use super::{api, models, run_migrations, Connection, SqliteStorageDatabase};
    use chrono::{DateTime, Utc};
    use diesel_async::pooled_connection::deadpool::Object;
    use rstest::fixture;
    use tempfile::TempDir;
    use tracing::trace;

    /// Fixture which creates new database as temp file.
    /// Removes database on drop.
    pub struct SqliteDatabaseFixture {
        /// Holds temp directory and removes it on drop.
        tmp: TempDir,

        /// Database connection.
        pub database: SqliteStorageDatabase,
    }

    impl SqliteDatabaseFixture {
        pub const FILENAME: &str = "test.carol.sqlite";

        /// Just an example entry of database.
        pub fn default_new_entry() -> models::NewFile {
            models::NewFile {
                source: url::Url::parse("http://localhost").unwrap().to_string(),
                cache_path: "/var/cache/file".to_string(),
                filename: None,
                created: DateTime::<Utc>::MIN_UTC,
                last_used: DateTime::<Utc>::MIN_UTC,
                store_policy: models::StorePolicy::StoreForever,
                store_policy_data: None,
                status: models::FileStatus::Pending,
            }
        }

        /// Create new empty temp database.
        pub async fn new() -> Self {
            trace!("creating CacheDatabaseFixture");
            let tmp = tempfile::tempdir().unwrap();
            let database_url = tmp
                .path()
                .join(Self::FILENAME)
                .to_str()
                .unwrap()
                .to_string();
            run_migrations(&database_url).await.unwrap();
            let database = SqliteStorageDatabase::connect_pool(&database_url, Some(4))
                .await
                .unwrap();
            Self { tmp, database }
        }

        /// Path to database file.
        pub fn database_url(&self) -> String {
            self.tmp
                .path()
                .join(Self::FILENAME)
                .to_str()
                .unwrap()
                .to_string()
        }

        /// Get one database connection instance.
        pub async fn conn(&self) -> Object<Connection> {
            self.database.pool.get().await.unwrap()
        }

        /// Insert new entry into current database fixture.
        pub async fn insert_entry(&self, new_entry: models::NewFile) -> models::File {
            trace!(
                "inserting entry into CacheDatabaseFixture: {:?}",
                &new_entry
            );
            let entry = api::insert(self.conn().await.as_mut(), new_entry)
                .await
                .unwrap();
            trace!("CacheDatabaseFixture now contains: {:?}", &entry);
            entry
        }
    }

    /// Create new empty database.
    #[fixture]
    pub async fn database() -> SqliteDatabaseFixture {
        SqliteDatabaseFixture::new().await
    }

    /// Create new database with a single entry.
    #[fixture]
    pub async fn database_with_single_entry(
        #[default(SqliteDatabaseFixture::default_new_entry())] new_entry: models::NewFile,
    ) -> (SqliteDatabaseFixture, models::File) {
        let database = database().await;
        let entry = database.insert_entry(new_entry).await;
        (database, entry)
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{database, database_with_single_entry, SqliteDatabaseFixture};
    use super::{establish_connection, models};
    use crate::database::{StorageDatabase, StorageDatabaseExt};
    use crate::error::NonUtf8PathError;
    use crate::file::{FileMetadata, FileSource, FileStatus, StorePolicy};
    use chrono::Utc;
    use rstest::rstest;
    use std::path::PathBuf;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_create_new_database() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("carol.sqlite");
        let db_path_str = db_path
            .as_os_str()
            .to_str()
            .ok_or(NonUtf8PathError)
            .unwrap();

        establish_connection(db_path_str)
            .await
            .expect("create new database");
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_connect_to_existing_database(#[future] database: SqliteDatabaseFixture) {
        establish_connection(&database.database_url())
            .await
            .expect("connect to existing database");
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_uri(#[future] database: SqliteDatabaseFixture) {
        assert_eq!(database.database.uri(), database.database_url());
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_debug_fmt(#[future] database: SqliteDatabaseFixture) {
        assert_eq!(
            format!("{:?}", database.database),
            format!(
                "SqliteStorageDatabase {{ database_url: \"{}\" }}",
                database.database_url()
            )
        );
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_store(#[future] database: SqliteDatabaseFixture) {
        let now = Utc::now();
        database
            .database
            .store(FileMetadata {
                source: FileSource::parse("somesource"),
                filename: None,
                path: PathBuf::from("/some/path"),
                store_policy: StorePolicy::StoreForever,
                created: now,
                last_used: now,
            })
            .await
            .expect("store");
    }

    #[rstest]
    #[case::real_id(None)]
    #[should_panic(expected = "get: DieselError(NotFound)")]
    #[case::wrong_id(Some(42))]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get(
        #[future] database_with_single_entry: (SqliteDatabaseFixture, models::File),
        #[case] id: Option<i32>,
    ) {
        let (fixture, inserted) = database_with_single_entry;
        let id = if let Some(id) = id { id } else { inserted.id };
        fixture.database.get(id.into()).await.expect("get");
    }

    #[rstest]
    #[case::real_id(None)]
    #[case::wrong_id(Some(42))]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_remove(
        #[future] database_with_single_entry: (SqliteDatabaseFixture, models::File),
        #[case] id: Option<i32>,
    ) {
        let (fixture, inserted) = database_with_single_entry;
        let id = if let Some(id) = id { id } else { inserted.id };
        fixture.database.remove(id.into()).await.expect("remove");
    }

    #[rstest]
    #[case::existing_source(None, 1)]
    #[case::not_existing_source(
        Some(FileSource::Custom("nosource".to_string())),
        0,
    )]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_select_by_source(
        #[future] database_with_single_entry: (SqliteDatabaseFixture, models::File),
        #[case] source: Option<FileSource>,
        #[case] expected_count: usize,
    ) {
        let (fixture, inserted) = database_with_single_entry;
        let source = if let Some(source) = source {
            source
        } else {
            FileSource::parse(&inserted.source)
        };
        let result = fixture
            .database
            .select_by_source(&source)
            .await
            .expect("select by source");
        assert_eq!(result.len(), expected_count);
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_update_status(
        #[future] database_with_single_entry: (SqliteDatabaseFixture, models::File),
    ) {
        let (fixture, inserted) = database_with_single_entry;
        let updated = fixture
            .database
            .update_status(inserted.id.into(), FileStatus::ToRemove)
            .await
            .expect("update status");
        assert_eq!(updated.status, FileStatus::ToRemove);
    }
}
