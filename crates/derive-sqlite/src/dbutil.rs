use super::{is_url_to_generate, Error, Lambda, Transform};
use anyhow::Context;
use prost::Message;
use proto_flow::RuntimeCheckpoint;
use rusqlite::Connection;

pub fn open(uri: &str, migrations: &[String]) -> anyhow::Result<(Connection, RuntimeCheckpoint)> {
    let conn = Connection::open(uri)?;

    // TODO(johnny): Lock it down.

    let () = set_optimal_journal_mode(&conn)?;
    run_script(&conn, BOOTSTRAP, "bootstrap").context("failed to bootstrap the database")?;
    apply_migrations(&conn, migrations)?;
    let runtime_checkpoint = query_checkpoint(&conn)?;

    Ok((conn, runtime_checkpoint))
}

fn apply_migrations(conn: &Connection, migrations: &[String]) -> anyhow::Result<()> {
    let max_applied: Option<usize> = conn
        .query_row("SELECT MAX(script_index) FROM flow_migrations", [], |row| {
            row.get(0)
        })
        .context("failed to query for applied migrations")?;

    // Apply any new migrations and record that we did so.
    for (index, block) in migrations
        .iter()
        .enumerate()
        .skip(max_applied.map(|m| m + 1).unwrap_or_default())
    {
        // Skip a migration block that needs to be generated.
        // This happens only during validation.
        if is_url_to_generate(&block) {
            continue;
        }

        run_script(&conn, block, &format!("migration at index {index}"))
            .with_context(|| format!("failed to apply database migration at index {index}"))?;

        conn.execute(
            "INSERT INTO flow_migrations (script_index, script) VALUES (?, ?);",
            rusqlite::params![index, block],
        )
        .context("failed to updated flow_migrations table for applied migrations")?;

        tracing::debug!(%index, %block, "applied SQLite migration");
    }

    Ok(())
}

pub fn build_transforms<'db>(
    conn: &'db Connection,
    transforms: &[Transform],
) -> anyhow::Result<Vec<(String, Vec<Lambda<'db>>)>> {
    let mut out = Vec::new();

    for Transform {
        name,
        source: _,
        block,
        params,
    } in transforms
    {
        out.push((name.clone(), Vec::new()));
        let (_, stack) = out.last_mut().unwrap();

        let block = if is_url_to_generate(block) {
            "SELECT 1;"
        } else {
            block
        };

        *stack = sql_block_to_statements(block)
            .and_then(|queries| {
                queries
                    .iter()
                    .map(|query| Lambda::new(conn, query, &params))
                    .collect()
            })
            .with_context(|| format!("lambda block of transform {name} is invalid"))?;
    }

    Ok(out)
}

fn run_script(conn: &Connection, block: &str, name: &str) -> anyhow::Result<()> {
    for statement in sql_block_to_statements(&block).context("script is not valid SQL")? {
        tracing::debug!(?statement, script=?name, "running script statement");
        let mut lambda = Lambda::new(conn, statement, &[])?;

        for row in lambda.invoke(&serde_json::Value::Null)? {
            let row = row?;
            tracing::debug!(?row, script=?name, "script output");
        }
    }
    Ok(())
}

fn query_checkpoint(conn: &Connection) -> anyhow::Result<RuntimeCheckpoint> {
    let runtime_checkpoint: Vec<u8> = conn
        .query_row(
            "SELECT CAST(checkpoint AS BLOB) FROM gazette_checkpoint",
            [],
            |row| row.get(0),
        )
        .context("failed to query the recovered runtime checkpoint")?;

    let runtime_checkpoint: RuntimeCheckpoint =
        Message::decode(bytes::Bytes::from(runtime_checkpoint))
            .context("failed to decode the recovered runtime checkpoint")?;

    Ok(runtime_checkpoint)
}

pub fn update_checkpoint(conn: &Connection, checkpoint: RuntimeCheckpoint) -> anyhow::Result<()> {
    let _ = conn
        .execute(
            "UPDATE gazette_checkpoint SET checkpoint = ?;",
            [checkpoint.encode_to_vec()],
        )
        .context("failed to update checkpoint")?;

    Ok(())
}

pub fn commit_and_begin(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
                COMMIT;
                BEGIN EXCLUSIVE;
                "#,
    )
    .context("failed to commit transaction")?;

    Ok(())
}

// Map a block of SQL into its constituent statements.
pub fn sql_block_to_statements(mut block: &str) -> Result<Vec<&str>, Error> {
    let mut statements = Vec::new();
    let mut pivot = 0;

    while !block.is_empty() {
        let c_stmt = match block[pivot..].find(";") {
            Some(i) => {
                pivot = pivot + i + 1;
                std::ffi::CString::new(&block[0..pivot])?
            }
            None => {
                if !block.chars().all(|c| c.is_whitespace()) {
                    return Err(Error::BlockTrailingContent {
                        trailing: block.to_string(),
                    });
                }
                return Ok(statements);

                // TODO(johnny): Previous implementation is commented below.
                // I'd much rather allow a soft-colon policy where we attempt to
                // interpret statements, but I can't come up with a good strategy
                // to get SQLite to tell us ahead of time that it's just comments
                // and not an actual statement.
                /*
                pivot = block.len();
                std::ffi::CString::new(format!("{block};"))?
                */
            }
        };

        if unsafe { rusqlite::ffi::sqlite3_complete(c_stmt.as_ptr()) } != 0 {
            // Ignore extra semi-colons ("statements" which are only whitespace).
            if c_stmt.as_bytes().trim_ascii() != b";" {
                statements.push(&block[0..pivot]);
            }
            block = &block[pivot..];
            pivot = 0;
        }
    }

    Ok(statements)
}

fn set_optimal_journal_mode(conn: &Connection) -> anyhow::Result<()> {
    // Query out SQLite's compile-time options.
    let sqlite_compile_options: String = conn
        .query_row(
            "SELECT json_group_array(compile_options) FROM pragma_compile_options",
            [],
            |row| row.get(0),
        )
        .context("failed to query sqlite compiled options")?;
    let sqlite_compile_options: Vec<String> =
        serde_json::from_str(&sqlite_compile_options).context("parsing compiled options")?;

    tracing::debug!(
        ?sqlite_compile_options,
        "queried SQLite compile time options"
    );

    // Select an appropriate journal_mode for the database.
    let journal_mode = if sqlite_compile_options
        .iter()
        .any(|o| o == "ENABLE_BATCH_ATOMIC_WRITE")
    {
        // In most cases, the rollback journal is not used at all and SQLite
        // issues direct writes to the main DB. For remaining cases,
        // TRUNCATE has fewest recovery log writes.
        "TRUNCATE"
    } else {
        // Prefer write-ahead log over rollback journal (less amplification).
        "WAL"
    };
    // rusqlite is a bit finicky about this pragma and we must use query_row.
    conn.query_row(&format!("PRAGMA journal_mode={journal_mode}"), [], |_row| {
        Ok(())
    })
    .context("failed to set journal_mode pragma")?;

    tracing::debug!(?journal_mode, "set SQLite journal_mode");

    Ok(())
}

const BOOTSTRAP: &str = r#"
    -- See: https://pkg.go.dev/go.gazette.dev/core/consumer/store-sqlite#hdr-Buffering_and_the_Synchronous_Pragma
    PRAGMA synchronous=FULL;

    -- Enable FAST secure delete, which writes zero pages on deleted data only opportunistically
    -- in contexts that don't increase IO, because zeros are highly compressible in RocksDB.
    PRAGMA secure_delete = FAST;

    -- Except for a brief moments during a commit, we always maintain
    -- an exclusive transaction over the database.
    BEGIN EXCLUSIVE;

    -- Prepare a table for gazette checkpoints.
    -- This exactly matches that of Gazette's store-sqlite implementation:
    -- https://github.com/gazette/core/blob/master/consumer/store-sqlite/store.go
    CREATE TABLE IF NOT EXISTS gazette_checkpoint (
        rowid INTEGER PRIMARY KEY DEFAULT 0 CHECK (rowid = 0), -- Permit just one row.
        checkpoint BLOB
    );
    INSERT OR IGNORE INTO gazette_checkpoint(rowid, checkpoint) VALUES (0, '');

    -- Prepare a table for tracking migrations.
    CREATE TABLE IF NOT EXISTS flow_migrations (
        script_index INTEGER PRIMARY KEY NOT NULL,
        script       TEXT NOT NULL
    );

    "#;

#[cfg(test)]
mod test {
    use super::*;
    use proto_flow::{runtime_checkpoint, RuntimeCheckpoint};

    #[test]
    fn bootstrap_and_migrate() {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        let mut migrations = vec![
            r#"
            CREATE TABLE one ( id INTEGER PRIMARY KEY NOT NULL );
        "#
            .to_string(),
            r#"
            INSERT INTO one (id) VALUES (4), (5);
            ALTER TABLE one ADD COLUMN value TEXT DEFAULT 'hello';
            CREATE TABLE two ( thing TEXT PRIMARY KEY NOT NULL );
            INSERT INTO two (thing) VALUES ('hi'), ('there');
        "#
            .to_string(),
        ];

        let (conn, _checkpoint) = open(tmp.path().to_str().unwrap(), &migrations).unwrap();

        update_checkpoint(
            &conn,
            RuntimeCheckpoint {
                sources: [(
                    "a/journal".to_string(),
                    runtime_checkpoint::Source {
                        read_through: 123,
                        ..Default::default()
                    },
                )]
                .into(),
                ..Default::default()
            },
        )
        .unwrap();

        conn.execute("update one set value = 'updated' where id = 5;", [])
            .unwrap();

        // We can close and then re-open with an added migration.
        commit_and_begin(&conn).unwrap();
        std::mem::drop(conn);

        migrations.push(
            r#"
            ALTER TABLE two ADD COLUMN other INTEGER DEFAULT 32;
            INSERT INTO two (thing, other) VALUES ('bye', 42);
        "#
            .to_string(),
        );

        let (conn, checkpoint) = open(tmp.path().to_str().unwrap(), &migrations).unwrap();

        assert_eq!(
            checkpoint.sources.len(),
            1,
            "we recovered an updated checkpoint"
        );

        let fixture_content: String = conn
            .query_row(
                r#"
                with rows as (
                    select json_object('id', id, 'value', value) as row from one
                    union all
                    select json_object('thing', thing, 'other', other) from two
                )
                select json_group_array(json(row)) from rows
                "#,
                [],
                |row| row.get(0),
            )
            .unwrap();
        insta::assert_snapshot!(fixture_content, @r###"[{"id":4,"value":"hello"},{"id":5,"value":"updated"},{"thing":"hi","other":32},{"thing":"there","other":32},{"thing":"bye","other":42}]"###);
    }

    #[test]
    fn mapping_sql_blocks_to_statements() {
        let statements = sql_block_to_statements(
            r#"
            -- Some comments.
            select '&;' as $foo
            from bar

            where baz = $bing and baz != ';';

            -- Next comment;
            update foobar set value = 'a ; new value';
            select 1 from thingy_without_trailing_semicolon
            /* Trailing
            ;
            comment */
            select 2;

            "#,
        )
        .unwrap();

        insta::assert_debug_snapshot!(statements, @r###"
        [
            "\n            -- Some comments.\n            select '&;' as $foo\n            from bar\n\n            where baz = $bing and baz != ';';",
            "\n\n            -- Next comment;\n            update foobar set value = 'a ; new value';",
            "\n            select 1 from thingy_without_trailing_semicolon\n            /* Trailing\n            ;\n            comment */\n            select 2;",
        ]
        "###);

        let statements = sql_block_to_statements(
            r#"
            select 1;

            -- This time, the statement has a closing colon.
            update foobar set value = 'a ; new value';
            "#,
        )
        .unwrap();

        insta::assert_debug_snapshot!(statements, @r###"
        [
            "\n            select 1;",
            "\n\n            -- This time, the statement has a closing colon.\n            update foobar set value = 'a ; new value';",
        ]
        "###);

        let statements = sql_block_to_statements(
            r#"
            -- Whitespace-only statements (extra semi-colons) are ignored.
            select 1;;
            ;
            select 2;  ;
            ;
            /* However, they're still included if they follow a comment */ ;
            "#,
        )
        .unwrap();

        insta::assert_debug_snapshot!(statements, @r###"
        [
            "\n            -- Whitespace-only statements (extra semi-colons) are ignored.\n            select 1;",
            "\n            select 2;",
            "\n            /* However, they're still included if they follow a comment */ ;",
        ]
        "###);

        insta::assert_snapshot!(sql_block_to_statements("select 1; \0 select 2;").unwrap_err(), @"SQL block contains illegal NULL characters");
    }
}
