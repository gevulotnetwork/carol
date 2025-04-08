//! Cache database.
//!
//! An SQLite database build with migrations from `./migrations`.

use diesel::{ConnectionError, ConnectionResult, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::time::{self, Duration};
use tracing::trace;

use crate::errors::DatabaseError;
use crate::RetryPolicy;

pub mod api;
pub mod models;
pub mod schema;

#[cfg(feature = "stresstest")]
#[cfg(test)]
#[cfg(not(tarpaulin))]
mod database_stresstest;

/// Inner SQLite connection type.
pub type Connection = SyncConnectionWrapper<SqliteConnection>;

/// Result of database operations.
pub type DatabaseResult<T> = Result<T, DatabaseError>;

const MIGRATIONS: EmbeddedMigrations =
    diesel_migrations::embed_migrations!("src/database/migrations");

const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

/// Establish connection with SQLite database and configure it with:
///
/// - `PRAGMA journal_mode = WAL`
/// - `PRAGMA synchronous = NORMAL`
/// - `PRAGMA busy_timeout = 10_000`
///
/// `retry` defines retry policy for pragma statements execution.
/// It is unrecommended to use [`RetryPolicy::None`], because multiple connections creations
/// will very likely result into "database is locked error".
fn establish_connection_inner(
    database_url: &str,
    retry: RetryPolicy,
) -> BoxFuture<ConnectionResult<Connection>> {
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
                let err = ConnectionError::CouldntSetupConfiguration(err);
                match retry {
                    RetryPolicy::None => Err(err),
                    RetryPolicy::Fixed { number, period } => {
                        trace!("SQLite connection configuration failed: {:?}", err);
                        let mut result: ConnectionResult<Connection> = Err(err);
                        for i in 0..number {
                            trace!("retrying, attempt #{}", i + 1);
                            time::sleep(period).await;
                            if let Err(err) = connection.batch_execute(&query).await {
                                // Last occured error will be returned
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
            }
        }
    };
    fut.boxed()
}

pub const DEFAULT_CONNECTION_RETRY: RetryPolicy = RetryPolicy::Fixed {
    number: 10, // We REALLY want this connection
    period: Duration::from_millis(100),
};

/// Establish connection to SQLite database with database_url.
///
/// Attempts to establish connection with existing database.
/// If database does not exists, it will be created.
///
/// When connection established, following configs will be applied:
/// - `PRAGMA journal_mode = WAL`
/// - `PRAGMA synchronous = NORMAL`
/// - `PRAGMA busy_timeout = 10_000`
///
/// Retry policy [`DEFAULT_CONNECTION_RETRY`] will be applied.
pub async fn establish_connection(database_url: &str) -> DatabaseResult<Connection> {
    Ok(establish_connection_inner(database_url, DEFAULT_CONNECTION_RETRY).await?)
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

/// Database fixtures. Helps in testing database-related code.
#[cfg(test)]
pub(crate) mod fixtures {
    use super::models::{CacheEntry, NewCacheEntry};
    use super::{api, establish_connection, run_migrations, Connection};
    use crate::{DateTime, FileStatus, Utc};
    use rstest::fixture;
    use tempfile::TempDir;
    use tracing::trace;

    /// Fixture which creates new database as temp file.
    /// Removes database on drop.
    pub(crate) struct CacheDatabaseFixture {
        /// Just holds temp directory, which will be removed on drop.
        #[allow(dead_code)]
        tmp: TempDir,

        /// Path to database `*.sqlite` file.
        pub db_path: String,

        /// Database connection.
        pub conn: Connection,
    }

    impl CacheDatabaseFixture {
        /// Just an example entry of database.
        pub fn default_new_entry() -> NewCacheEntry {
            NewCacheEntry {
                url: "http://localhost".to_string(),
                cache_path: "/var/cache/file".to_string(),
                created: DateTime::<Utc>::MIN_UTC,
                expires: None,
                status: FileStatus::Pending,
                ref_count: 0,
            }
        }

        /// Create new empty temp database.
        pub async fn new() -> Self {
            trace!("creating CacheDatabaseFixture");
            let tmp = tempfile::tempdir().unwrap();
            let db_path = tmp.path().join("carol.sqlite");
            let db_path = db_path.to_str().unwrap().to_string();
            run_migrations(&db_path).await.unwrap();
            let db = establish_connection(&db_path).await.unwrap();
            Self {
                tmp,
                db_path,
                conn: db,
            }
        }

        /// Insert new entry into current database fixture.
        pub async fn insert_entry(&mut self, new_entry: NewCacheEntry) -> CacheEntry {
            trace!(
                "inserting entry into CacheDatabaseFixture: {:?}",
                &new_entry
            );
            let entry = unsafe { api::insert_unsafe(&mut self.conn, new_entry).await.unwrap() };
            trace!("CacheDatabaseFixture now contains: {:?}", &entry);
            entry
        }
    }

    // TODO: use in-memory database for fixture

    /// Create new empty database.
    #[fixture]
    pub(crate) async fn database() -> CacheDatabaseFixture {
        CacheDatabaseFixture::new().await
    }

    /// Create new database with a single entry.
    #[fixture]
    pub(crate) async fn database_with_single_entry(
        #[default(CacheDatabaseFixture::default_new_entry())] new_entry: NewCacheEntry,
    ) -> (CacheDatabaseFixture, CacheEntry) {
        let mut database = database().await;
        let entry = database.insert_entry(new_entry).await;
        (database, entry)
    }
}

#[cfg(test)]
mod tests {
    use super::establish_connection;
    use super::fixtures::{database, CacheDatabaseFixture};
    use crate::errors::NonUtf8PathError;
    use rstest::rstest;
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
    async fn test_connect_to_existing_database(#[future] database: CacheDatabaseFixture) {
        establish_connection(&database.db_path)
            .await
            .expect("connect to existing database");
    }
}
