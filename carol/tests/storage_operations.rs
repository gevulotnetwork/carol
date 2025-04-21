use anyhow::anyhow;
use bytes::Bytes;
use carol::{EvictionPolicy, FileLockMode, StorageConfig, StorageManager, StorePolicy};

#[tokio::test]
async fn test_storage_operations() -> anyhow::Result<()> {
    let storage_dir = tempfile::tempdir()?;
    let database_dir = tempfile::tempdir()?;

    let storage_path = storage_dir.path().to_path_buf();
    let database_path = database_dir.path().join("carol.test.sqlite");
    let database_url = database_path
        .as_os_str()
        .to_str()
        .ok_or(anyhow!("non UTF-8 database path"))?;

    let config = StorageConfig {
        eviction_policy: EvictionPolicy::Lru,
    };

    let manager =
        StorageManager::init_with_config(database_url, &storage_path, Some(4), config).await?;

    println!("{:#?}", manager);
    println!("{:#?}", manager.config());

    let file = manager
        .add_file(
            "some-source".into(),
            StorePolicy::StoreForever,
            None,
            Bytes::from_static("test".as_bytes()),
        )
        .await?;

    let _lock = file.lock(FileLockMode::Shared)?;

    let content = std::fs::read_to_string(&file.metadata.path)?;

    assert_eq!(&content, "test");

    let maybe_file = manager.find_by_source(&"some-source".into()).await?;
    assert!(maybe_file.is_some());

    let same_file = manager
        .add_file(
            "some-source".into(),
            StorePolicy::StoreForever,
            None,
            Bytes::from_static("test".as_bytes()),
        )
        .await?;

    assert_eq!(same_file.id, file.id);

    Ok(())
}
