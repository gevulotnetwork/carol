//! Stress-testing SQLite cache database.
//!
//! This test is pretty heavy, so it is hidden behind `stresstest` feature.
//! It is not going to be run by default.

use crate::database::{self, api, Connection};
use crate::errors::DatabaseError;
use crate::FileStatus;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_cache_database_stress_testing() {
    // This test will run a bunch of random operations
    // on the database from multiple clients at the same time.
    // The goal is to make sure we will get no unexpected errors.

    // num of workers
    const N: usize = 24;

    // num of operations to perform
    const COUNT: usize = 3000;

    // testing on new empty database
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("carol.sqlite");

    database::run_migrations(db_path.as_os_str().to_str().unwrap())
        .await
        .expect(&format!("run migrations on database"));

    // primary keys collection shared accross all workers
    let pks = Arc::new(RwLock::new(Vec::new()));

    let unknown_errors = Arc::new(RwLock::new(0u64));

    // Syncronously inilialize database clients
    // Asyncronous initlization often fails with "database is locked"
    let mut tasks = JoinSet::new();
    for i in 0..N {
        let db = database::establish_connection(db_path.as_os_str().to_str().unwrap())
            .await
            .expect(&format!("init database {}", i));
        tasks.spawn(inner(db, pks.clone(), unknown_errors.clone()));
    }

    // this function will create a new db client
    // and spam random db operations
    async fn inner(
        mut db: Connection,
        pks: Arc<RwLock<Vec<i32>>>,
        unknown_errors: Arc<RwLock<u64>>,
    ) {
        use rand::distr::Alphanumeric;
        use rand::seq::IndexedRandom;
        use rand::Rng;

        // Operation codes:
        // 1 - new_entry
        // 2 - get_entry
        // 3 - remove_entry
        // 4 - remove_entry_unsafe
        // 5 - get_all
        // 6 - get_by_url
        // 7 - update_status
        // 8 - increment_ref
        // 9 - decrement_ref

        for _ in 0..COUNT {
            // lock primary key set while performing operation
            let (pk, action, url, cache_path) = {
                let pks_copy = pks.read().await.clone();
                let mut rng = rand::rng();
                let action = rng.random_range(1..10);
                let pk = pks_copy.choose(&mut rng).map(|i| *i);
                let url = format!(
                    "http://localhost/{}",
                    (0..15)
                        .map(|_| rng.sample(Alphanumeric) as char)
                        .collect::<String>()
                );
                let cache_path = format!(
                    "/var/cache/{}",
                    (0..15)
                        .map(|_| rng.sample(Alphanumeric) as char)
                        .collect::<String>()
                );
                (pk, action, url, cache_path)
            };

            let allow_some_errors = async |err: DatabaseError| -> DatabaseError {
                match &err {
                    DatabaseError::DieselError(diesel::result::Error::NotFound) => err,
                    // this includes "database is locker" error
                    DatabaseError::DieselError(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::Unknown,
                        _,
                    )) => {
                        *unknown_errors.write().await += 1;
                        err
                    }
                    err => panic!("{:?}", err),
                }
            };

            match action {
                1 => {
                    match api::new_entry(&mut db, &url, &cache_path, None).await {
                        Ok(entry) => {
                            pks.write().await.push(entry.id);
                        }
                        Err(err) if err.is_unique_violation() => {
                            // it's okay, but very unlikely to happened
                        }
                        Err(err) => {
                            let _ = allow_some_errors(err).await;
                        }
                    }
                }
                2 => {
                    if let Some(pk) = pk {
                        let _ = api::get_entry(&mut db, pk).await.map_err(allow_some_errors);
                    }
                }
                3 => {
                    if let Some(pk) = pk {
                        let _ =
                            api::remove_entry(&mut db, pk)
                                .await
                                .map_err(async |err| match err {
                                    DatabaseError::DieselError(diesel::result::Error::NotFound) => {
                                        err
                                    }
                                    DatabaseError::RemoveError(..) => err,
                                    _ => allow_some_errors(err).await,
                                });
                        pks.write().await.retain(|&i| i != pk);
                    }
                }
                4 => {
                    if let Some(pk) = pk {
                        let _ = unsafe { api::delete_unsafe(&mut db, pk) }
                            .await
                            .map_err(allow_some_errors);
                        pks.write().await.retain(|&i| i != pk);
                    }
                }
                5 => {
                    api::get_all(&mut db).await.expect("get_all never fails");
                }
                6 => {
                    let _ = api::get_by_url(&mut db, &url)
                        .await
                        .expect("get_by_url never fails");
                }
                7 => {
                    if let Some(pk) = pk {
                        let _ = api::update_status(&mut db, pk, FileStatus::ToRemove)
                            .await
                            .map_err(allow_some_errors);
                    }
                }
                8 => {
                    if let Some(pk) = pk {
                        let _ = api::increment_ref(&mut db, pk)
                            .await
                            .map_err(allow_some_errors);
                    }
                }
                9 => {
                    if let Some(pk) = pk {
                        let _ = api::increment_ref(&mut db, pk)
                            .await
                            .map_err(allow_some_errors);
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    tasks.join_all().await;

    let total_errors = unknown_errors.read().await;
    const THRESHOLD: u64 = N as u64 * COUNT as u64 / 1000; // 0.1% of all operations are allowed to fail
    if *total_errors > THRESHOLD {
        panic!("total_errors = {}", total_errors);
    }
}
