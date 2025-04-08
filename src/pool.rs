//! Pool of Carol clients.
//!
//! This is based on [`deadpool::managed`].
//!
//! ## Example
//!
//! ```rust
//! # async fn test(database_url: &str, cache_dir: &str) {
//! use carol::pool::{Pool, PoolManager};
//!
//! // Build clients pool
//! let pool_manager = PoolManager::new(database_url, cache_dir);
//! let pool = Pool::builder(pool_manager).build().unwrap();
//!
//! // Get Carol client from pool
//! let mut client = pool.get().await.unwrap();
//!
//! // Use it as usually
//! let file = client.get("https://example.com").await.unwrap();
//! # }
//! ```

use std::path::Path;

use deadpool::managed;

use crate::errors::Error;
use crate::{Client, ClientBuilder};

/// Manager responsible for creating new Carol clients.
pub struct PoolManager {
    builder: ClientBuilder,
}

impl PoolManager {
    /// Create new manager which will create default client.
    pub fn new<P>(database_url: &str, cache_dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        let builder = ClientBuilder::new(database_url, cache_dir);
        Self::from_client_builder(builder)
    }

    /// Create new manager which will create configured clients.
    pub fn from_client_builder(builder: ClientBuilder) -> Self {
        Self { builder }
    }
}

impl managed::Manager for PoolManager {
    type Type = Client;
    type Error = Error;

    async fn create(&self) -> Result<Client, Error> {
        self.builder.clone().build().await
    }

    async fn recycle(&self, _: &mut Client, _: &managed::Metrics) -> managed::RecycleResult<Error> {
        Ok(())
    }
}

/// Pool of Carol clients.
///
/// Alias for [`deadpool::managed::Pool`].
pub type Pool = managed::Pool<PoolManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_build() {
        let tmp_database = tempfile::tempdir().unwrap();
        let database = tmp_database.path().join("carol.sqlite");
        let database_path = database.as_os_str().to_str().unwrap().to_string();
        let tmp_cache_dir = tempfile::tempdir().unwrap();

        let mgr = PoolManager::new(&database_path, tmp_cache_dir.path());
        let pool = Pool::builder(mgr).max_size(16).build().expect("build pool");

        let mut conn = pool.get().await.expect("get connection from pool");

        let file = conn
            .get("https://example.com")
            .await
            .expect("get example file");

        file.release().await.expect("release example file");
        conn.remove("https://example.com")
            .await
            .expect("remove example file");
    }

    #[tokio::test]
    async fn test_small_pool() {
        let tmp_database = tempfile::tempdir().unwrap();
        let database = tmp_database.path().join("carol.sqlite");
        let database_path = database.as_os_str().to_str().unwrap().to_string();
        let tmp_cache_dir = tempfile::tempdir().unwrap();

        let mgr = PoolManager::new(&database_path, tmp_cache_dir.path());
        let pool = Pool::builder(mgr).max_size(1).build().expect("build pool");
        let client1 = pool.get().await.expect("get connection from pool");
        drop(client1);
        // Basically we check here that connection was succesfully recycled
        let _client2 = pool.get().await.expect("get second connection from pool");
    }
}
