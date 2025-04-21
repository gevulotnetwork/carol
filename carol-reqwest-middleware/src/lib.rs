//! Caching middleware for [`reqwest`] backed by [`carol`] storage.
//!
//! # Example
//!
//! ```rust
//! # async fn test(cache_dir: &str, database_url: &str) {
//! use carol_reqwest_middleware::storage::{File, StorageManager, StorePolicy};
//! use carol_reqwest_middleware::CarolMiddleware;
//!
//! let storage_manager = StorageManager::init(database_url, cache_dir, None).await.unwrap();
//! let carol_middleware = CarolMiddleware {
//!     storage_manager,
//!     store_policy: StorePolicy::ExpiresAfterNotUsedFor {
//!         duration: std::time::Duration::from_secs(3600),
//!     },
//! };
//!
//! let reqwest_client = reqwest::Client::builder().build().unwrap();
//! let client = reqwest_middleware::ClientBuilder::new(reqwest_client).with(carol_middleware).build();
//! let response = client.get("https://example.com").send().await.unwrap();
//! let file = response.json::<File>().await.unwrap();
//!
//! // Downloaded file is stored at 'file.metadata.path'
//! let content = std::fs::read(&file.metadata.path);
//! # }
//! ```

use std::path::Path;

use async_trait::async_trait;
use content_disposition::parse_content_disposition;
use http::header::CONTENT_DISPOSITION;
use http::Extensions;
use reqwest_middleware::reqwest::{Body, Request, Response, ResponseBuilderExt};
use reqwest_middleware::{Error as MidError, Middleware, Next};
use serde::Serialize;

#[doc(no_inline)]
pub use carol as storage;

use carol::sqlite::SqliteStorageDatabase;
use carol::{StorageDatabaseExt, StorageManager, StorePolicy};

pub struct CarolMiddleware<D: StorageDatabaseExt = SqliteStorageDatabase> {
    pub storage_manager: StorageManager<D>,
    pub store_policy: StorePolicy,
}

#[async_trait]
impl<D> Middleware for CarolMiddleware<D>
where
    D: StorageDatabaseExt + 'static,
    D::Uri: Serialize + Send,
{
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let mut builder = http::Response::builder();
        let url = req.url().to_owned();
        builder = builder.url(url.clone());

        let origin_response = next.run(req, extensions).await?.error_for_status()?;

        builder = builder.status(origin_response.status());
        builder = builder.version(origin_response.version());
        for header in origin_response.headers() {
            builder = builder.header(header.0, header.1);
        }

        let filename = get_filename(&origin_response);
        let stream = origin_response.bytes_stream();

        let file = self
            .storage_manager
            .add_file_from_stream(url.into(), self.store_policy, filename, stream)
            .await
            .map_err(MidError::middleware)?;

        let body = Body::from(serde_json::to_string(&file).map_err(MidError::middleware)?);
        let response = Response::from(builder.body(body).map_err(MidError::middleware)?);
        Ok(response)
    }
}

/// Try getting file name from HTTP response.
fn get_filename(response: &Response) -> Option<String> {
    // Try getting file name from Content-Disposition first
    if let Some(value) = response.headers().get(CONTENT_DISPOSITION) {
        if let Ok(value_str) = value.to_str() {
            if let Some(filename) = parse_content_disposition(value_str).filename_full() {
                return Some(filename);
            }
        }
    }
    // Try deducing file name from URL
    Path::new(response.url().path())
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::CarolMiddleware;
    use carol::{File, StorageManager, StorePolicy};
    use http_test_server::TestServer;
    use rstest::{fixture, rstest};
    use std::time::Duration;
    use tokio::fs;

    const DEFAULT_PATH: &str = "/hello.txt";
    const DEFAULT_CONTENT: &str = "Hello world";
    const DEFAULT_HEADERS: &[(&str, &str)] = &[("Content-Type", "text/plain")];

    /// Test HTTP server.
    #[fixture]
    #[once]
    pub fn test_server(
        #[default(DEFAULT_PATH)] path: &'static str,
        #[default(DEFAULT_CONTENT)] content: &'static str,
        #[default(DEFAULT_HEADERS)] headers: &[(&'static str, &'static str)],
    ) -> TestServer {
        use http_test_server::http::{Method, Status};
        let server = TestServer::new().unwrap();
        let resource = server.create_resource(path);
        resource
            .status(Status::OK)
            .method(Method::GET)
            .body(content);
        for (header_name, header_value) in headers {
            resource.header(header_name, header_value);
        }
        server
    }

    #[rstest]
    #[tokio::test]
    async fn test_middleware(test_server: &TestServer) {
        let temp = tempfile::tempdir().unwrap();
        let database_path = temp.path().join("carol.sqlite");
        let database_url = database_path.to_str().unwrap();
        let cache_dir = temp.path().join("files");
        fs::create_dir(&cache_dir).await.unwrap();
        let url = format!("http://localhost:{}/hello.txt", test_server.port());

        let storage_manager = StorageManager::init(database_url, &cache_dir, None)
            .await
            .expect("init storage manager");
        let reqwest_client = reqwest::Client::builder().build().unwrap();
        let client = reqwest_middleware::ClientBuilder::new(reqwest_client)
            .with(CarolMiddleware {
                storage_manager,
                store_policy: StorePolicy::ExpiresAfterNotUsedFor {
                    duration: Duration::from_secs(3600),
                },
            })
            .build();

        let response = client.get(&url).send().await.expect("get URL");
        let file = response.json::<File>().await.expect("deserialize response");
        assert_eq!(file.metadata.filename, Some("hello.txt".to_string()));
        assert_eq!(file.metadata.source.as_str(), &url);
        let content = fs::read_to_string(&file.metadata.path)
            .await
            .expect("read file content");
        assert_eq!(&content, DEFAULT_CONTENT);
        println!("{:#?}", file);
    }
}
