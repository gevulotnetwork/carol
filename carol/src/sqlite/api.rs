//! Convenient API to interact with cache database.
//!
//! Basically just fancy wrappers around transactions on [`Connection`].

use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::trace;

use super::models::{File, FileStatus, NewFile};
use super::schema::files::dsl::{self, files};
use super::{Connection, DatabaseResult, PrimaryKey};

/// Insert new entry to database.
pub async fn insert(connection: &mut Connection, new_entry: NewFile) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                trace!("INSERT {:?}", new_entry);
                let inserted = diesel::insert_into(files)
                    .values(&new_entry)
                    .get_result::<File>(conn)
                    .await?;
                Ok(inserted)
            }
            .scope_boxed()
        })
        .await
}

/// Get entry from database by primary key.
pub async fn get(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<File> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT * WHERE id={}", pk);
                files.find(pk).select(File::as_select()).first(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Delete entry from database.
pub async fn delete(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<()> {
    connection
        .immediate_transaction(|conn| {
            async {
                trace!("DELETE WHERE id={}", pk);
                diesel::delete(files.find(pk)).execute(conn).await?;
                Ok(())
            }
            .scope_boxed()
        })
        .await
}

/// Get all cache entries from database.
pub async fn get_all(connection: &mut Connection) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT *");
                files.select(File::as_select()).get_results(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Get entry from database by source (URL or custom source).
pub async fn get_by_source(connection: &mut Connection, source: &str) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT * WHERE source={}", source);
                files
                    .filter(dsl::source.eq(source))
                    .select(File::as_select())
                    .get_results(conn)
                    .await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Get entry from database by its cache path field.
#[allow(dead_code)]
pub async fn get_by_cache_path(
    connection: &mut Connection,
    cache_path: &str,
) -> DatabaseResult<Option<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT * WHERE cache_path={}", cache_path);
                files
                    .filter(dsl::cache_path.eq(cache_path))
                    .select(File::as_select())
                    .first(conn)
                    .await
                    .optional()
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Get all cache entries with given `status`.
#[allow(dead_code)]
pub async fn get_by_status(
    connection: &mut Connection,
    status: FileStatus,
) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT * WHERE status={}", status);
                files
                    .filter(dsl::status.eq(status))
                    .select(File::as_select())
                    .get_results(conn)
                    .await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Update status of entry. Returns updated entry.
pub async fn update_status(
    connection: &mut Connection,
    pk: PrimaryKey,
    status: FileStatus,
) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                trace!("UPDATE SET status={} WHERE id={}", status, pk);
                diesel::update(files.find(pk))
                    .set(dsl::status.eq(status))
                    .get_result(conn)
                    .await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

pub async fn order_by_created(connection: &mut Connection) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT * ORDER BY created");
                files.order_by(dsl::created).get_results(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

pub async fn order_by_last_used(connection: &mut Connection) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT * ORDER BY last_used");
                files.order_by(dsl::last_used).get_results(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Get all "stale" cache entries from the database.
#[allow(dead_code)]
pub async fn get_all_stale(_connection: &mut Connection) -> DatabaseResult<Vec<File>> {
    // connection
    //     .transaction(|conn| {
    //         async {
    //             // let min_months_predicate = a::min_unit
    //             //     .eq(Some("Months"))
    //             //     .and(a::min_duration.le(Some(param1)));
    //             // let min_years_predicate = a::min_unit
    //             //     .eq(Some("Years"))
    //             //     .and(a::min_duration.le(Some(param1)));

    //             // query = query
    //             //     .filter(min_months_predicate.or(min_years_predicate))
    //             //     .filter(a::max_duration.ge(Some(param2)));
    //             // let x = conn.batch_execute("").await;

    //             trace!("SELECT * WHERE (store_policy=ExpiresAfter AND store_policy_data + created < NOW())");
    //             let expired_after = dsl::store_policy
    //                 .eq(StorePolicy::ExpiresAfter)
    //                 .and((dsl::store_policy_data).gt(123));
    //             let expired_after_not_used = dsl::store_policy
    //                 .eq(StorePolicy::ExpiresAfterNotUsedFor)
    //                 .and(dsl::store_policy_data.gt(123));
    //             files
    //                 .filter(expired_after.or(expired_after_not_used))
    //                 .select(File::as_select())
    //                 .get_results(conn)
    //                 .await
    //         }
    //         .scope_boxed()
    //     })
    //     .await
    //     .map_err(Into::into)
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::StorageDatabaseError;
    use crate::sqlite::error::DatabaseError;
    use crate::sqlite::fixtures::{database, database_with_single_entry, SqliteDatabaseFixture};
    use crate::sqlite::models::StorePolicy;
    use chrono::{TimeDelta, Utc};
    use rstest::rstest;
    use tracing_test::traced_test;

    #[rstest(database as db_fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_insert(#[future] db_fixture: SqliteDatabaseFixture) {
        let entry = insert(
            db_fixture.conn().await.as_mut(),
            NewFile {
                source: "http://localhost".to_string(),
                cache_path: "/var/cache/file".to_string(),
                filename: None,
                created: Utc::now(),
                last_used: Utc::now(),
                store_policy: StorePolicy::StoreForever,
                store_policy_data: None,
                status: FileStatus::default(),
            },
        )
        .await
        .expect("add new entry");
        assert_eq!(entry.source, "http://localhost".to_string());
        assert_eq!(entry.cache_path, "/var/cache/file".to_string());
        assert_eq!(entry.status, FileStatus::Pending);
        assert_eq!(entry.store_policy, StorePolicy::StoreForever);
        assert_eq!(entry.store_policy_data, None);
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_insert_failure(#[future] fixture: (SqliteDatabaseFixture, File)) {
        let (db_fixture, inserted_entry) = fixture;

        let result = insert(
            db_fixture.conn().await.as_mut(),
            NewFile {
                source: "http://localhost".to_string(),
                cache_path: inserted_entry.cache_path,
                filename: None,
                created: Utc::now(),
                last_used: Utc::now(),
                store_policy: StorePolicy::StoreForever,
                store_policy_data: None,
                status: FileStatus::default(),
            },
        )
        .await;

        assert!(
            result
                .as_ref()
                .is_err_and(DatabaseError::is_unique_violation),
            "cache path must be unique"
        );
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get(#[future] fixture: (SqliteDatabaseFixture, File)) {
        let (db_fixture, inserted_entry) = fixture;
        let entry = get(db_fixture.conn().await.as_mut(), inserted_entry.id)
            .await
            .expect("get entry");
        assert_eq!(entry, inserted_entry);
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get_by_url(#[future] fixture: (SqliteDatabaseFixture, File)) {
        let (db_fixture, inserted_entry) = fixture;
        let entries = get_by_source(
            db_fixture.conn().await.as_mut(),
            url::Url::parse("http://localhost").unwrap().as_str(),
        )
        .await
        .expect("get entry by url");
        assert_eq!(entries, vec![inserted_entry]);
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get_by_cache_path(#[future] fixture: (SqliteDatabaseFixture, File)) {
        let (db_fixture, inserted_entry) = fixture;
        let entry = get_by_cache_path(db_fixture.conn().await.as_mut(), "/var/cache/file")
            .await
            .expect("get entry")
            .expect("get some entry by cache path");
        assert_eq!(entry, inserted_entry);
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get_all(#[future] fixture: (SqliteDatabaseFixture, File)) {
        let (db_fixture, inserted_entry) = fixture;
        let all = get_all(db_fixture.conn().await.as_mut())
            .await
            .expect("get all entries");
        assert_eq!(all, vec![inserted_entry]);
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_get_by_status(#[future] fixture: (SqliteDatabaseFixture, File)) {
        let (db_fixture, inserted_entry) = fixture;
        let all = get_by_status(db_fixture.conn().await.as_mut(), FileStatus::Pending)
            .await
            .expect("get all pending entries");
        assert_eq!(all, vec![inserted_entry]);
    }

    #[rstest(database_with_single_entry as fixture)]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_update_status(
        #[future] fixture: (SqliteDatabaseFixture, File),
        #[values(
            FileStatus::Pending,
            FileStatus::Ready,
            FileStatus::Corrupted,
            FileStatus::ToRemove
        )]
        status: FileStatus,
    ) {
        let (db_fixture, inserted_entry) = fixture;
        update_status(db_fixture.conn().await.as_mut(), inserted_entry.id, status)
            .await
            .expect("update file status");
        let entry = get(db_fixture.conn().await.as_mut(), inserted_entry.id)
            .await
            .unwrap();
        assert_eq!(entry.status, status);
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_delete(
        #[future]
        #[from(database_with_single_entry)]
        fixture: (SqliteDatabaseFixture, File),
    ) {
        let (db_fixture, inserted_entry) = fixture;
        delete(db_fixture.conn().await.as_mut(), inserted_entry.id)
            .await
            .expect("remove entry");
        let all = get_all(db_fixture.conn().await.as_mut()).await.unwrap();
        assert!(all.is_empty());
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_order_by_created(
        #[future]
        #[from(database)]
        fixture: SqliteDatabaseFixture,
    ) {
        let now = Utc::now();

        let file1 = insert(
            fixture.conn().await.as_mut(),
            NewFile {
                created: now,
                cache_path: "1".to_string(),
                ..SqliteDatabaseFixture::default_new_entry()
            },
        )
        .await
        .unwrap();
        let file2 = insert(
            fixture.conn().await.as_mut(),
            NewFile {
                created: now - TimeDelta::seconds(1),
                cache_path: "2".to_string(),
                ..SqliteDatabaseFixture::default_new_entry()
            },
        )
        .await
        .unwrap();
        let file3 = insert(
            fixture.conn().await.as_mut(),
            NewFile {
                created: now + TimeDelta::seconds(1),
                cache_path: "3".to_string(),
                ..SqliteDatabaseFixture::default_new_entry()
            },
        )
        .await
        .unwrap();
        let all = order_by_created(fixture.conn().await.as_mut())
            .await
            .expect("order by created");
        assert_eq!(all, vec![file2, file1, file3]);
    }

    #[rstest]
    #[tokio::test]
    #[traced_test]
    #[awt]
    async fn test_order_by_last_used(
        #[future]
        #[from(database)]
        fixture: SqliteDatabaseFixture,
    ) {
        let now = Utc::now();

        let file1 = insert(
            fixture.conn().await.as_mut(),
            NewFile {
                last_used: now,
                cache_path: "1".to_string(),
                ..SqliteDatabaseFixture::default_new_entry()
            },
        )
        .await
        .unwrap();
        let file2 = insert(
            fixture.conn().await.as_mut(),
            NewFile {
                last_used: now - TimeDelta::seconds(1),
                cache_path: "2".to_string(),
                ..SqliteDatabaseFixture::default_new_entry()
            },
        )
        .await
        .unwrap();
        let file3 = insert(
            fixture.conn().await.as_mut(),
            NewFile {
                last_used: now + TimeDelta::seconds(1),
                cache_path: "3".to_string(),
                ..SqliteDatabaseFixture::default_new_entry()
            },
        )
        .await
        .unwrap();
        let all = order_by_last_used(fixture.conn().await.as_mut())
            .await
            .expect("order by last used");
        assert_eq!(all, vec![file2, file1, file3]);
    }
}
