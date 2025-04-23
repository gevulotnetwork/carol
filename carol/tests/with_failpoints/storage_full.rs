use carol::StorageManager;
use failpoints::FailScenario;

/// This tests that eviction is properly triggered by storage when encountering StorageFull error.
#[test_log::test(tokio::test)]
async fn storage_full() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("carol.sqlite");
    let database_url = database_path.to_str().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();

    let storage_manager = StorageManager::init(database_url, &cache_dir, None)
        .await
        .expect("init storage manager");

    // Just some file to evict
    // If no files will be in the storage, the eviction will fail
    storage_manager
        .add_file("0".into(), Default::default(), None, [].as_slice().into())
        .await
        .unwrap();

    let scenario = FailScenario::setup();

    // Configure failpoint to return StorageFull error once and then return Ok()
    // This way we emulate the scenario when there is no enough space in the storage now,
    // but after eviction it is fine.
    failpoints::cfg("write-chunk-storage-full", "1*return->off").unwrap();

    storage_manager
        .add_file("1".into(), Default::default(), None, [].as_slice().into())
        .await
        .expect("add file");

    scenario.teardown();
}
