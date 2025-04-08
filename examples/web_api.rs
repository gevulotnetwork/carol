use std::time::Duration;

use carol::maintenance::{MaintenanceOpts, MaintenanceRunner};
use carol::pool::{Pool, PoolManager};
use carol::{Client, File, RetryPolicy};
use tracing_subscriber::EnvFilter;
use warp::Filter;

const DATABASE_URL: &str = "web-api.carol.sqlite";
const CACHE_DIR: &str = "web-api.carol.files";
const MAINTENANCE_PERIOD: Duration = Duration::from_secs(60);

/// Example of web API for requesting URLs to download.
/// Also maintenance is running in a separate task.
///
/// Server accepts requests like:
///
/// ```json
/// {
///   "urls": [
///     "http://example.com"
///   ]
/// }
/// ```
///
/// Then downloads requested URLs through Carol and returs response like:
///
/// ```json
/// {
///   "files": [
///     {
///       "url": "http://example.com",
///       "cache_path": "/home/user/carol/web-api.carol.files/f0e6a6a97042a4f1f1c87f5f7d44315b2d852c2df5c7991cc66241bf7072d1c4",
///       "created": "2025-04-02T16:15:08.714973713Z"
///     }
///   ]
/// }
/// ```
///
/// Run and test with cURL:
///
/// ```shell
/// curl -X POST http://127.0.0.1:8080/api -H "Content-Type: application/json" -d '{"urls": ["http://example.com"]}'
/// ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();

    // This client builder will be used in our clients pool
    let mut client_builder = Client::builder(DATABASE_URL, CACHE_DIR);
    client_builder
        .default_file_duration(Duration::from_secs(30))
        .download_retry_policy(RetryPolicy::Fixed {
            number: 3,
            period: Duration::from_secs(1),
        });
    // Create pool of Carol clients
    let cache_pool = Pool::builder(PoolManager::from_client_builder(client_builder)).build()?;

    // Create task for running maintenance every 1 min
    let maintenance = run_maintenance(cache_pool.clone(), MAINTENANCE_PERIOD);

    // Setup API server
    let api = warp::post()
        .and(warp::path("api"))
        .and(warp::body::json())
        .and(warp::any().map(move || cache_pool.clone()))
        .and_then(web_api::handle_request)
        .recover(web_api::handle_rejection);
    let web_server = warp::serve(api).run(([127, 0, 0, 1], 8080));

    tokio::select! {
        _ = web_server => {
            eprintln!("web server stopped");
        }
        result = maintenance => {
            eprintln!("maintenance stopped");
            result?;
        }
    };

    Ok(())
}

async fn run_maintenance(cache_pool: Pool, period: Duration) -> anyhow::Result<()> {
    // Right now maintenance is run against a single client
    // This may be changed in favor of clients pool in the future
    let mut client = cache_pool.get().await?;

    let opts = MaintenanceOpts::builder()
        .run_cache_cleaning(true)
        .find_corrupted(true)
        .remove_corrupted(true)
        .prune_dangling(true)
        .build()?;
    let mut runner = MaintenanceRunner::new(client.as_mut(), opts);

    loop {
        tokio::time::sleep(period).await;
        if let Err(err) = runner.run_once().await {
            eprintln!("Error during maintenance run: {:?}", err);
        }
    }
}

async fn get_file(clients_pool: Pool, url: String) -> anyhow::Result<File> {
    Ok(clients_pool.get().await?.get(url).await?)
}

// Some `wrap` code for Web API definition
mod web_api {
    use super::get_file;
    use carol::pool::Pool;
    use carol::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;
    use tracing::info;
    use warp::reply;

    #[derive(Deserialize, Debug)]
    pub struct Request {
        urls: Vec<String>,
    }

    /// Just a serializable mirror of [`carol::File`] to return to user
    #[derive(Serialize, Debug)]
    pub struct File {
        url: String,
        cache_path: PathBuf,
        created: DateTime<Utc>,
    }

    impl From<carol::File> for File {
        fn from(value: carol::File) -> Self {
            Self {
                url: value.url().to_string(),
                cache_path: value.cache_path().to_path_buf(),
                created: *value.created(),
            }
        }
    }

    #[derive(Serialize, Debug)]
    pub struct Response {
        files: Vec<File>,
    }

    #[derive(Serialize, Debug)]
    pub struct ServerError {
        error: String,
    }

    impl warp::reject::Reject for ServerError {}

    pub async fn handle_request(
        req: Request,
        cache_pool: Pool,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        info!("requested {} URLs: {:?}", req.urls.len(), req.urls);

        let mut tasks = tokio::task::JoinSet::new();
        for url in &req.urls {
            let task = get_file(cache_pool.clone(), url.clone());
            tasks.spawn(task);
        }

        let files = tasks
            .join_all()
            .await
            .into_iter()
            .map(|res| res.map(File::from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                warp::reject::custom(ServerError {
                    error: err.to_string(),
                })
            })?;

        let response = Response { files };
        Ok(reply::json(&response))
    }

    pub async fn handle_rejection(
        err: warp::Rejection,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        if let Some(e) = err.find::<ServerError>() {
            let reply = reply::json(e);
            let reply = reply::with_status(reply, warp::http::StatusCode::INTERNAL_SERVER_ERROR);
            let reply = reply::with_header(reply, "Content-Type", "application/json");
            return Ok(reply);
        }
        Err(err)
    }
}
