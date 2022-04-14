use sqlx::migrate::Migrate;
use sqlx::migrate::MigrationType;
use sqlx::Connection;
use sqlx::Executor;
use sqlx::Row;

// It would be A Bad Thing if tests were run against a production database,
// because tests add and revert migrations as part of their execution.
// We lock it down to a prescribed local database which is put under test.
const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

// TODO(johnny): This is merely a proof-of-concept, demonstrating that we can
// up- and down-migrate the database at will to make assertions about
// the behaviors of those migrations.
// A lot of boiler plate could probably be removed here by standardizing
// on some particular query shapes (to_row_json()?).
#[tokio::test]
async fn test_foobar() {
    let migrator = sqlx::migrate!("../../migrations");

    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .expect("connect");

    conn.ensure_migrations_table()
        .await
        .expect("migrations table");

    conn.execute(include_str!("../../supa_mocks.sql"))
        .await
        .expect("supa mocks setup");

    migrate_to(&mut conn, 0, &migrator)
        .await
        .expect("migrate to v0 failed");

    migrate_to(&mut conn, 1, &migrator)
        .await
        .expect("migrate to v1 failed");

    sqlx::query("insert into connectors (image_name) select * from unnest($1);")
        .bind(vec!["foo/bar", "baz", "bing/forty/two"])
        .execute(&mut conn)
        .await
        .unwrap();

    migrate_to(&mut conn, 2, &migrator)
        .await
        .expect("migrate to v2 failed");

    let out = sqlx::query("select image_name from connectors_copy;")
        .fetch_all(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect::<Vec<String>>();

    assert_eq!(
        out,
        vec![
            "copy/foo/bar".to_string(),
            "copy/baz".to_string(),
            "copy/bing/forty/two".to_string(),
        ]
    );
}

async fn migrate_to(
    conn: &mut sqlx::postgres::PgConnection,
    version: i64,
    migrator: &sqlx::migrate::Migrator,
) -> sqlx::Result<()> {
    let mut applied = conn
        .list_applied_migrations()
        .await?
        .into_iter()
        .map(|a| a.version)
        .max()
        .unwrap_or_default();

    while applied > version {
        let down = migrator
            .migrations
            .iter()
            .find(|m| {
                m.version == applied && matches!(m.migration_type, MigrationType::ReversibleDown)
            })
            .expect("down migration");
        conn.revert(down).await?;
        applied -= 1;
    }

    while applied < version {
        let up = migrator
            .migrations
            .iter()
            .find(|m| {
                m.version == applied + 1 && matches!(m.migration_type, MigrationType::ReversibleUp)
            })
            .expect("up migration");
        conn.apply(up).await?;
        applied += 1;
    }

    Ok(())
}
