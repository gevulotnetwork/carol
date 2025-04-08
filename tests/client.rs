//! Client integration tests.

use std::process::{Command, Stdio};
use tokio::fs;

use carol::pool::{Pool, PoolManager};
use carol::{Client, File};

/// Just an example of files usage.
async fn cat_files<'a, I>(files: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = &'a File>,
{
    Command::new("cat")
        .args(files.into_iter().map(|file| file.cache_path().as_os_str()))
        .stdout(Stdio::null())
        .status()?;
    Ok(())
}

#[tokio::test]
async fn test_with_one_file() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("carol.sqlite");
    let db_path_str = format!("{}", db_path.display());
    let cache_dir = tmp.path().join("files");
    fs::create_dir(&cache_dir).await?;

    let mut client = Client::init(&db_path_str, &cache_dir).await?;

    let source_url = "https://example.com";

    // This variable will keep a reference to source URL, preventing it from removal.
    let file = client.get(source_url).await?;

    cat_files([&file]).await?;

    Ok(())
}

#[tokio::test]
async fn test_with_many_files() -> anyhow::Result<()> {
    // This test shows how to use connection pool to fetch multiple files at the same time

    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("carol.sqlite");
    let db_path_str = format!("{}", db_path.display());
    let cache_dir = tmp.path().join("files");
    fs::create_dir(&cache_dir).await?;

    let source_urls = &["https://example.com", "http://example.com"];

    // Create clients pool
    let clients_pool = Pool::builder(PoolManager::new(&db_path_str, cache_dir)).build()?;

    // Define tasks to fetch all files from `source_urls`
    async fn get_file(clients_pool: Pool, url: &str) -> anyhow::Result<File> {
        let mut client = clients_pool.get().await?;
        Ok(client.get(url).await?)
    }
    let mut tasks = tokio::task::JoinSet::new();
    for url in source_urls {
        let task = get_file(clients_pool.clone(), url);
        tasks.spawn(task);
    }
    let files = tasks
        .join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    cat_files(&files).await?;

    Ok(())
}
