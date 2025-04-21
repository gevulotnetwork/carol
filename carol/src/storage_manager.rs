use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use rand::seq::SliceRandom;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::database::{StorageDatabase, StorageDatabaseError, StorageDatabaseExt};
use crate::error::StorageError;
use crate::file::{
    File, FileLockError, FileLockMode, FileMetadata, FileSource, FileStatus, StorePolicy,
};
use crate::sqlite::{self, run_migrations, SqliteStorageDatabase};
use crate::storage_config::StorageConfig;

/// Storage manager. This is an adapter to interact with Carol storage.
///
/// To all the methods which add new file to a storage like:
///
/// - [`add_file`][Self::add_file]
/// - [`add_file_from_stream`][Self::add_file_from_stream]
/// - [`copy_local_file`][Self::copy_local_file]
/// - [`move_local_file`][Self::move_local_file]
///
/// the following applies:
///
/// - "create" and "last used" timestamps of the file will be set to `Utc::now()`
/// - path to file inside the storage is defined by [`path_from_source`][Self::path_from_source]
///   method
#[derive(Clone, Debug)]
pub struct StorageManager<D: StorageDatabase = SqliteStorageDatabase> {
    db: D,
    dir: PathBuf,
    config: StorageConfig,
}

impl<D: StorageDatabase> StorageManager<D> {
    /// Returns reference to config of this storage.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Generate cached file path from [`FileSource`].
    ///
    /// Applies SHA256 to `source` string representation
    /// to generate the file name in cache directory.
    pub fn path_from_source(&self, source: &FileSource) -> PathBuf {
        self.dir.join(sha256::digest(source))
    }
}

impl<D: StorageDatabaseExt> StorageManager<D> {
    /// Add new file to storage. Content of the file is read from `stream`.
    pub async fn add_file_from_stream<S, E>(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        mut stream: S,
    ) -> Result<File, StorageError<D::Error>>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: StdError + 'static + Send + Sync,
    {
        let path = self.path_from_source(&source);
        let now = Utc::now();
        let metadata = FileMetadata {
            source: source.clone(),
            filename,
            path: path.clone(),
            store_policy,
            created: now,
            last_used: now,
        };

        match self.db.store(metadata).await {
            Ok(id) => {
                let mut run = async || -> Result<File, StorageError<D::Error>> {
                    let mut output = fs::File::create_new(&path).await?;
                    while let Some(chunk_result) = stream.next().await {
                        let chunk = chunk_result.map_err(StorageError::custom)?;
                        self.write_chunk(&chunk, &mut output).await?;
                    }
                    let file = self.db.update_status(id, FileStatus::Ready).await?;
                    Ok(file)
                };

                let revert = async || -> Result<(), StorageError<D::Error>> {
                    fs::remove_file(&path).await?;
                    self.db.remove(id).await?;
                    Ok(())
                };

                match run().await {
                    Ok(file) => Ok(file),
                    Err(err) => {
                        revert().await?;
                        Err(err)
                    }
                }
            }
            Err(err) if err.is_unique_violation() => {
                // FIXME: looping is probably not the best approach
                let file = loop {
                    match self.find_by_source(&source).await? {
                        Some(file) if file.status == FileStatus::Ready => {
                            break file;
                        }
                        Some(file) if file.status == FileStatus::Pending => {
                            time::sleep(Duration::from_secs(1)).await;
                        }
                        _ => {
                            return Err(StorageError::AwaitingError);
                        }
                    }
                };
                // TODO: check that file is not stale
                Ok(file)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Add new file to storage by **copying** it from local path.
    ///
    /// If you are using local path as `source`, keep in mind that sources are unique in the
    /// storage. Because of that the same call for a modified local file **will not update** the
    /// file in the storage.
    pub async fn copy_local_file(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        path: impl AsRef<Path>,
    ) -> Result<File, StorageError<D::Error>> {
        let file = fs::File::open(path.as_ref()).await?;
        let stream =
            FramedRead::new(file, BytesCodec::new()).map(|item| item.map(BytesMut::freeze));
        self.add_file_from_stream(source, store_policy, filename, stream)
            .await
    }

    /// Move local file into the storage. This will remove the source file.
    ///
    /// If the storage directory and a local file are on a different devices, file will be copied
    /// with [`Self::copy_local_file`]. In that case there is a chance that the file will be
    /// succesfully copied into the storage, but won't be removed from source path due to some
    /// error. If one knows that storage is located on a different device, then it is recommended
    /// to use [`Self::copy_local_file`] directly instead.
    pub async fn move_local_file(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        path: impl AsRef<Path>,
    ) -> Result<File, StorageError<D::Error>> {
        let source_path = path.as_ref();
        let path = self.path_from_source(&source);

        match fs::rename(source_path, &path).await {
            Err(err) if err.kind() == IoErrorKind::CrossesDevices => {
                let file = self
                    .copy_local_file(source, store_policy, filename, source_path)
                    .await?;
                fs::remove_file(source_path).await?;
                Ok(file)
            }
            Err(err) => Err(err.into()),
            Ok(_) => {
                let now = Utc::now();
                let metadata = FileMetadata {
                    source: source.clone(),
                    filename,
                    path: path.clone(),
                    store_policy,
                    created: now,
                    last_used: now,
                };
                let run = async || -> Result<File, StorageError<D::Error>> {
                    let id = self.db.store(metadata).await?;
                    let file = self.db.update_status(id, FileStatus::Ready).await?;
                    Ok(file)
                };
                let revert = async || -> Result<(), StorageError<D::Error>> {
                    fs::rename(&path, source_path).await.map_err(Into::into)
                };

                match run().await {
                    Ok(file) => Ok(file),
                    Err(err) => {
                        revert().await?;
                        Err(err)
                    }
                }
            }
        }
    }

    /// Add new file to storage with given content.
    pub async fn add_file(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        content: Bytes,
    ) -> Result<File, StorageError<D::Error>> {
        let stream = tokio_stream::once(Ok::<_, std::convert::Infallible>(content));
        self.add_file_from_stream(source, store_policy, filename, stream)
            .await
    }

    /// Find file in storage by its source.
    pub async fn find_by_source(
        &self,
        source: &FileSource,
    ) -> Result<Option<File>, StorageError<D::Error>> {
        let files = self.db.select_by_source(source).await?;
        // Because of the way self.path_from_source() works, sources
        // are also expected to be unique.
        debug_assert!(files.len() <= 1);
        Ok(files.into_iter().next())
    }

    /// Attempts to write chunk of bytes into file. If "no space left" error occurs, tries to evict
    /// as many files from storage as needed to free enough space.
    async fn write_chunk(&self, chunk: &Bytes, output: &mut fs::File) -> Result<(), IoError> {
        match output.write_all(chunk).await {
            Err(err)
                if err.kind() == IoErrorKind::StorageFull
                    || err.kind() == IoErrorKind::QuotaExceeded =>
            {
                // If "no space left" occured, evict some file from storage and retry writing
                if self.evict_one_file().await.is_ok() {
                    Box::pin(self.write_chunk(chunk, output)).await
                } else {
                    Err(err)
                }
            }
            res => res,
        }
    }

    /// Evicts one file from storage using current eviction policy.
    /// Return `Ok(())` if some file was succesfully evicted and error otherwise.
    async fn evict_one_file(&self) -> Result<(), StorageError<D::Error>> {
        // TODO: optimize preformance (avoid selecting all files at once)
        let files = match self.config.eviction_policy {
            crate::EvictionPolicy::Lru => self.db.order_by_last_used().await?,
            crate::EvictionPolicy::Fifo => self.db.order_by_created().await?,
            crate::EvictionPolicy::Random => {
                let mut files = self.db.select_all().await?;
                let mut rng = rand::rng();
                files.shuffle(&mut rng);
                files
            }
        };
        for file in files {
            match file.try_lock(FileLockMode::Exclusive) {
                Ok(_lock) => {
                    fs::remove_file(&file.metadata.path).await?;
                    self.db.remove(file.id).await?;
                    return Ok(());
                }
                Err(FileLockError::AlreadyLocked) => continue,
                Err(FileLockError::Io(err)) => return Err(err.into()),
            }
        }
        Err(StorageError::EvictionFailed)
    }
}

impl StorageManager {
    /// Initialize new storage manager with SQLite database.
    ///
    /// If the storage database doesn't exist yet, it will be created.
    ///
    /// # Arguments
    ///
    /// - `database_url` - URL of SQLite database to use. Typically this is just a path,
    ///   e.g. `/path/to/db.sqlite`.
    /// - `dir` - path to storage directory, where the actual files will reside.
    ///   This **must be** an absolute path. This directory **must** exist.
    /// - `pool_size` - size of the database connection pool. If `None`, defaults to `cpu_count * 4`.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `dir` is not absolute
    /// - `dir` does not exists or is not a directory
    /// - connection to database failed
    /// - running migrations on the database failed
    pub async fn init(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        pool_size: Option<usize>,
    ) -> Result<Self, StorageError<sqlite::error::DatabaseError>> {
        Self::init_with_config(database_url, dir, pool_size, StorageConfig::default()).await
    }

    /// Provide custom storage configuration. See [`Self::init`] for more info.
    pub async fn init_with_config(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        pool_size: Option<usize>,
        config: StorageConfig,
    ) -> Result<Self, StorageError<sqlite::error::DatabaseError>> {
        if !dir.as_ref().is_absolute() {
            return Err(StorageError::StorageDirectoryPathIsNotAbsolute);
        }
        let metadata = fs::metadata(dir.as_ref()).await?;
        if !metadata.is_dir() {
            return Err(IoError::new(
                IoErrorKind::NotADirectory,
                "storage path is not a directory",
            )
            .into());
        }
        let dir = dir.as_ref().to_path_buf();
        run_migrations(database_url.as_ref()).await?;
        let db = SqliteStorageDatabase::connect_pool(database_url.as_ref(), pool_size).await?;
        Ok(Self { db, dir, config })
    }
}

#[cfg(test)]
mod tests {
    use super::StorageManager;
    use crate::database::mocks::MockStorageDatabaseExt;
    use crate::file::{File, FileId, FileMetadata, FileSource, FileStatus, StorePolicy};
    use crate::storage_config::{EvictionPolicy, StorageConfig};
    use bytes::Bytes;
    use chrono::{DateTime, TimeDelta, Utc};
    use tokio::fs;

    #[derive(Debug)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    impl std::error::Error for TestError {}

    #[tokio::test]
    async fn test_add_file_from_stream() {
        // Set up initial data
        let database_url = "someurl".to_string();
        let tmp = tempfile::tempdir().unwrap();
        let source = FileSource::Custom("somesource".to_string());
        let store_policy = StorePolicy::StoreForever;
        let data: Vec<Result<Bytes, TestError>> =
            vec![Ok(Bytes::from("hello ")), Ok(Bytes::from("world"))];
        let stream = futures_util::stream::iter(data);
        let filename = None;
        let path = tmp
            .path()
            .join("6f87d01289b1845908a7c7ccd578fddbbcefd29f6144bbab658baa9f6aae2809");

        // Set up database mock
        let mut mock = MockStorageDatabaseExt::new();
        let file_id = FileId::from(1i32);
        let metadata = FileMetadata {
            source: source.clone(),
            filename: filename.clone(),
            path: path.clone(),
            store_policy,
            created: Utc::now(),
            last_used: Utc::now(),
        };

        let metadata_clone = metadata.clone();
        mock.expect_store()
            .withf(move |metadata| {
                metadata.filename == metadata_clone.filename
                    && metadata.path == metadata_clone.path
                    && metadata.source == metadata_clone.source
                    && metadata.store_policy == store_policy
            })
            .return_once(move |_| Ok(file_id));

        let database_url_clone = database_url.clone();
        mock.expect_update_status()
            .withf(move |id, new_status| *id == file_id && *new_status == FileStatus::Ready)
            .return_once(move |id, status| {
                Ok(File {
                    database: database_url_clone,
                    id,
                    status,
                    metadata,
                })
            });

        // Create manager
        let manager = StorageManager::<MockStorageDatabaseExt> {
            db: mock,
            dir: tmp.path().to_path_buf(),
            config: Default::default(),
        };

        let file = manager
            .add_file_from_stream(source.clone(), store_policy, filename.clone(), stream)
            .await
            .expect("add file from stream");

        assert_eq!(file.database, database_url);
        assert_eq!(file.id, file_id);
        assert_eq!(file.status, FileStatus::Ready);
        assert_eq!(file.metadata.filename, filename);
        assert_eq!(file.metadata.path, path);
        assert_eq!(file.metadata.source, source);
        assert_eq!(file.metadata.store_policy, store_policy);
        let content = fs::read_to_string(&file.metadata.path)
            .await
            .expect("read content");
        assert_eq!(content.as_str(), "hello world");
    }

    #[tokio::test]
    async fn test_copy_local_file() {
        // Set up initial data
        let localtmp = tempfile::tempdir().unwrap();
        let localfile_path = localtmp.path().join("localfile");
        fs::write(&localfile_path, "hello world").await.unwrap();

        let database_url = "someurl".to_string();
        let tmp = tempfile::tempdir().unwrap();
        let source = FileSource::Custom("somesource".to_string());
        let store_policy = StorePolicy::StoreForever;
        let filename = None;
        let path = tmp
            .path()
            .join("6f87d01289b1845908a7c7ccd578fddbbcefd29f6144bbab658baa9f6aae2809");

        // Set up database mock
        let mut mock = MockStorageDatabaseExt::new();
        let file_id = FileId::from(1i32);
        let metadata = FileMetadata {
            source: source.clone(),
            filename: filename.clone(),
            path: path.clone(),
            store_policy,
            created: Utc::now(),
            last_used: Utc::now(),
        };

        let metadata_clone = metadata.clone();
        mock.expect_store()
            .withf(move |metadata| {
                metadata.filename == metadata_clone.filename
                    && metadata.path == metadata_clone.path
                    && metadata.source == metadata_clone.source
                    && metadata.store_policy == store_policy
            })
            .return_once(move |_| Ok(file_id));

        let database_url_clone = database_url.clone();
        mock.expect_update_status()
            .withf(move |id, new_status| *id == file_id && *new_status == FileStatus::Ready)
            .return_once(move |id, status| {
                Ok(File {
                    database: database_url_clone,
                    id,
                    status,
                    metadata,
                })
            });

        // Create manager
        let manager = StorageManager::<MockStorageDatabaseExt> {
            db: mock,
            dir: tmp.path().to_path_buf(),
            config: Default::default(),
        };

        let file = manager
            .copy_local_file(
                source.clone(),
                store_policy,
                filename.clone(),
                &localfile_path,
            )
            .await
            .expect("copy local file");

        assert_eq!(file.database, database_url);
        assert_eq!(file.id, file_id);
        assert_eq!(file.status, FileStatus::Ready);
        assert_eq!(file.metadata.filename, filename);
        assert_eq!(file.metadata.path, path);
        assert_eq!(file.metadata.source, source);
        assert_eq!(file.metadata.store_policy, store_policy);
        let content = fs::read_to_string(&file.metadata.path)
            .await
            .expect("read content");
        assert_eq!(content.as_str(), "hello world");
    }

    #[tokio::test]
    async fn test_evict_one_file() {
        let tmp = tempfile::tempdir().unwrap();

        let stored_file_path_1 = tmp.path().join("file1");
        fs::File::create(&stored_file_path_1).await.unwrap();

        let stored_file_path_2 = tmp.path().join("file2");
        fs::File::create(&stored_file_path_2).await.unwrap();

        let file1 = File {
            database: "someurl".to_string(),
            id: 1.into(),
            status: FileStatus::Ready,
            metadata: FileMetadata {
                path: stored_file_path_1.clone(),
                last_used: DateTime::<Utc>::MIN_UTC,
                source: FileSource::Custom("".to_string()),
                filename: Default::default(),
                store_policy: Default::default(),
                created: Default::default(),
            },
        };

        let file2 = File {
            database: "someurl".to_string(),
            id: 2.into(),
            status: FileStatus::Ready,
            metadata: FileMetadata {
                path: stored_file_path_2.clone(),
                last_used: DateTime::<Utc>::MIN_UTC + TimeDelta::seconds(1),
                source: FileSource::Custom("".to_string()),
                filename: Default::default(),
                store_policy: Default::default(),
                created: Default::default(),
            },
        };

        let mut mock = MockStorageDatabaseExt::new();

        mock.expect_order_by_last_used()
            .return_once(move || Ok(vec![file1, file2]));

        mock.expect_remove()
            .withf(|id| *id == 1i32.into())
            .return_once(|_| Ok(()));

        let config = StorageConfig {
            eviction_policy: EvictionPolicy::Lru,
        };

        let manager = StorageManager::<MockStorageDatabaseExt> {
            db: mock,
            dir: tmp.path().to_path_buf(),
            config,
        };

        manager.evict_one_file().await.expect("evict one file");

        assert!(!stored_file_path_1.exists());
        assert!(stored_file_path_2.exists());
    }
}
