use carol::StorageManager;
use failpoints::FailScenario;

/// This tests that move_local_file copies the file in case of CrossesDevices error.
#[test_log::test(tokio::test)]
async fn move_crosses_devices() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("carol.sqlite");
    let database_url = database_path.to_str().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let local_file_path = temp.path().join("local-file");
    std::fs::File::create(&local_file_path).unwrap();

    let storage_manager = StorageManager::init(database_url, &cache_dir, None)
        .await
        .expect("init storage manager");

    let scenario = FailScenario::setup();

    // Configure failpoint to return CrossesDevices error
    // This way we emulate the scenario when user attempts to move the file into storage,
    // but the file and the storage directory are on different filesystems.
    failpoints::cfg("move-local-file-crosses-devices", "return").unwrap();

    storage_manager
        .move_local_file("1".into(), Default::default(), None, &local_file_path)
        .await
        .expect("move local file");

    assert!(!local_file_path.exists());

    scenario.teardown();
}
