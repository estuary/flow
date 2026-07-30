//! The reference connector's destination: a SQLite file.
//!
//! SQLite is chosen for what it gives the suite for free — real transactions, so
//! "commits during `StartCommit`" and "commits during `Store`" are genuinely
//! different behaviours rather than a simulation of them; and a real fence
//! table, so a zombie's stale write is rejected by the destination rather than by
//! a check the connector performs on itself. It costs no credentials and no
//! cloud spend, which is what lets scenario development happen locally.
//!
//! This module is the IO layer: every routine here is a SQL statement or two.
//! The decisions about *when* to call them belong to the class state machines in
//! `super`.

use anyhow::Context;
use rusqlite::OptionalExtension;

/// A document to be written to a materialized resource.
pub struct Row {
    pub binding: usize,
    /// The key tuple as its JSON array text, used verbatim as the primary key of
    /// a standard binding's table.
    pub key: String,
    pub doc: String,
    pub delete: bool,
}

/// A materialized resource of the destination.
#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub delta: bool,
}

pub struct Store {
    conn: rusqlite::Connection,
}

impl Store {
    /// Open (creating if absent) the destination, and ensure the connector's own
    /// bookkeeping tables exist.
    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating destination directory {parent:?}"))?;
        }
        let conn = rusqlite::Connection::open(path)
            .with_context(|| format!("opening destination {path:?}"))?;

        // WAL plus a generous busy timeout: a split task's two shards, and a zombie
        // racing the live instance, are all separate processes writing this one
        // file. Without these, contention surfaces as SQLITE_BUSY and the suite
        // would report a connector failure where the behaviour under test is a
        // fence rejection.
        //
        // The busy timeout goes first, and that ordering is the whole point:
        // switching journal mode takes a brief exclusive lock, so setting WAL while
        // a sibling shard holds the file fails outright with "database is locked"
        // unless the connection has already been told to wait. Getting this backwards
        // made every shard-split scenario fail — the second child died during `Open`
        // and the leader reported an unexpected EOF from its fan-in.
        conn.busy_timeout(std::time::Duration::from_secs(30))
            .context("setting the busy timeout")?;

        // Set through a query rather than the batch below because it *returns* the
        // mode it settled on, which `execute_batch` refuses.
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
            .context("enabling WAL journaling")?;

        conn.execute_batch(
            "PRAGMA synchronous = FULL;

             CREATE TABLE IF NOT EXISTS _flow_fence (
                 key_begin  INTEGER NOT NULL,
                 key_end    INTEGER NOT NULL,
                 nonce      INTEGER NOT NULL,
                 checkpoint BLOB,
                 PRIMARY KEY (key_begin, key_end)
             );

             CREATE TABLE IF NOT EXISTS _flow_staged (
                 ord     INTEGER PRIMARY KEY AUTOINCREMENT,
                 txn     INTEGER NOT NULL,
                 shard   INTEGER NOT NULL,
                 tbl     TEXT    NOT NULL,
                 delta   INTEGER NOT NULL,
                 key     TEXT    NOT NULL,
                 doc     TEXT    NOT NULL,
                 del     INTEGER NOT NULL
             );

             CREATE TABLE IF NOT EXISTS _flow_applied_txn (
                 shard INTEGER NOT NULL,
                 txn   INTEGER NOT NULL,
                 PRIMARY KEY (shard, txn)
             );

             CREATE TABLE IF NOT EXISTS _flow_counter (
                 shard    INTEGER NOT NULL,
                 tbl      TEXT    NOT NULL,
                 appended INTEGER NOT NULL,
                 PRIMARY KEY (shard, tbl)
             );

             CREATE TABLE IF NOT EXISTS _flow_spec (
                 version TEXT PRIMARY KEY,
                 applied TEXT NOT NULL
             );",
        )
        .context("initializing destination bookkeeping")?;

        Ok(Self { conn })
    }

    /// Claim `[key_begin, key_end)`, returning the nonce this session holds and
    /// the runtime checkpoint it should resume from.
    ///
    /// Fencing is the whole point: a later `Open` raises the nonce of every range
    /// that overlaps it, so an earlier session's `commit` — which checks that the
    /// nonce it was handed is still current — can no longer succeed. Bumping
    /// *overlapping* ranges rather than only the identical one is what fences a
    /// shard's parent after a split, since the parent's range strictly contains
    /// both children's.
    ///
    /// A split also has to inherit a checkpoint: the child's range has never been
    /// opened, so the checkpoint to resume from is the one stored against the
    /// narrowest range containing it. A join (two ranges collapsing into one)
    /// inherits nothing and falls back to the recovery log — that asymmetry is
    /// real and is why the join scenarios assert only on the destination, never
    /// on which checkpoint the connector chose.
    pub fn fence(&self, key_begin: u32, key_end: u32) -> anyhow::Result<(i64, Option<Vec<u8>>)> {
        let txn = self.conn.unchecked_transaction()?;

        let overlapping: Vec<(u32, u32, i64, Option<Vec<u8>>)> = {
            let mut stmt = txn.prepare(
                "SELECT key_begin, key_end, nonce, checkpoint FROM _flow_fence
                 WHERE key_begin <= ?2 AND key_end >= ?1",
            )?;
            stmt.query_map((key_begin, key_end), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };

        let nonce = 1 + overlapping.iter().map(|(_, _, n, _)| *n).max().unwrap_or(0);

        let checkpoint = overlapping
            .iter()
            .filter(|(kb, ke, _, _)| *kb <= key_begin && *ke >= key_end)
            .min_by_key(|(kb, ke, _, _)| (*ke as u64) - (*kb as u64))
            .and_then(|(_, _, _, cp)| cp.clone());

        for (kb, ke, _, _) in &overlapping {
            txn.execute(
                "UPDATE _flow_fence SET nonce = ?3 WHERE key_begin = ?1 AND key_end = ?2",
                (kb, ke, nonce),
            )?;
        }

        txn.execute(
            "INSERT INTO _flow_fence (key_begin, key_end, nonce, checkpoint)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (key_begin, key_end) DO UPDATE SET nonce = ?3",
            (key_begin, key_end, nonce, &checkpoint),
        )?;
        txn.commit()?;

        Ok((nonce, checkpoint))
    }

    /// A standard binding's table is keyed and upserted; a delta binding's is an
    /// append-only log whose `ord` preserves delivery order, which is what makes
    /// a duplicated document visible as an extra row.
    pub fn ensure_table(&self, table: &Table) -> anyhow::Result<()> {
        let ddl = if table.delta {
            format!(
                "CREATE TABLE IF NOT EXISTS \"{}\" (
                     ord INTEGER PRIMARY KEY AUTOINCREMENT,
                     key TEXT NOT NULL,
                     doc TEXT NOT NULL)",
                table.name
            )
        } else {
            format!(
                "CREATE TABLE IF NOT EXISTS \"{}\" (
                     key TEXT PRIMARY KEY,
                     doc TEXT NOT NULL)",
                table.name
            )
        };
        self.conn
            .execute_batch(&ddl)
            .with_context(|| format!("creating table {}", table.name))?;
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> anyhow::Result<()> {
        self.conn
            .execute_batch(&format!("DROP TABLE IF EXISTS \"{name}\""))
            .with_context(|| format!("dropping table {name}"))?;
        Ok(())
    }

    pub fn load(&self, table: &Table, key: &str) -> anyhow::Result<Option<String>> {
        let doc = self
            .conn
            .query_row(
                &format!("SELECT doc FROM \"{}\" WHERE key = ?1", table.name),
                (key,),
                |r| r.get::<_, String>(0),
            )
            .optional()
            .with_context(|| format!("loading from {}", table.name))?;
        Ok(doc)
    }

    /// Apply `rows` and the runtime checkpoint in one transaction, refusing if
    /// our fence is no longer current.
    ///
    /// `check_fence` is the switch behind the `skip-fence-check` defect: with it
    /// off, a zombie's commit lands.
    pub fn commit(
        &self,
        key_begin: u32,
        key_end: u32,
        nonce: i64,
        checkpoint: Option<&[u8]>,
        rows: &[(Table, Row)],
        check_fence: bool,
    ) -> anyhow::Result<()> {
        let txn = self.conn.unchecked_transaction()?;

        if check_fence {
            let current: Option<i64> = txn
                .query_row(
                    "SELECT nonce FROM _flow_fence WHERE key_begin = ?1 AND key_end = ?2",
                    (key_begin, key_end),
                    |r| r.get(0),
                )
                .optional()?;

            anyhow::ensure!(
                current == Some(nonce),
                "fenced off: destination holds nonce {current:?} but this session holds {nonce}"
            );
        }

        write_rows(&txn, rows)?;

        if let Some(checkpoint) = checkpoint {
            txn.execute(
                "UPDATE _flow_fence SET checkpoint = ?3 WHERE key_begin = ?1 AND key_end = ?2",
                (key_begin, key_end, checkpoint),
            )?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Durably stage `rows` against `txn_id` without making them visible in the
    /// destination tables. The post-commit-apply class's `Store` path.
    pub fn stage(&self, shard: u32, txn_id: i64, rows: &[(Table, Row)]) -> anyhow::Result<()> {
        let txn = self.conn.unchecked_transaction()?;
        {
            let mut stmt = txn.prepare(
                "INSERT INTO _flow_staged (txn, shard, tbl, delta, key, doc, del)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for (table, row) in rows {
                stmt.execute((
                    txn_id,
                    shard,
                    &table.name,
                    table.delta as i64,
                    &row.key,
                    &row.doc,
                    row.delete as i64,
                ))?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Apply this shard's rows staged for `txn_id` to their tables.
    ///
    /// With `idempotent`, a claim on `(shard, txn)` is taken first — skipping the
    /// whole apply if it already exists — and the staged rows are then forgotten.
    /// Together those make a replayed `Acknowledge` a no-op, which the runtime is
    /// entitled to rely on.
    ///
    /// Without it, the rows are applied and *left in place*: no claim to notice
    /// that they have already landed, and no deletion to make a second attempt
    /// find nothing. That is the `non-idempotent-acknowledge` defect, and it models
    /// the real-world shape of it — staged files that the connector forgets to
    /// retire — rather than a contrived one.
    pub fn apply_staged(&self, shard: u32, txn_id: i64, idempotent: bool) -> anyhow::Result<bool> {
        let txn = self.conn.unchecked_transaction()?;

        if idempotent {
            let claimed = txn.execute(
                "INSERT OR IGNORE INTO _flow_applied_txn (shard, txn) VALUES (?1, ?2)",
                (shard, txn_id),
            )?;
            if claimed == 0 {
                txn.commit()?;
                return Ok(false);
            }
        }

        let rows = {
            let mut stmt = txn.prepare(
                "SELECT tbl, delta, key, doc, del FROM _flow_staged
                 WHERE shard = ?1 AND txn = ?2 ORDER BY ord",
            )?;
            let rows = stmt
                .query_map((shard, txn_id), |r| {
                    Ok((
                        Table {
                            name: r.get::<_, String>(0)?,
                            delta: r.get::<_, i64>(1)? != 0,
                        },
                        Row {
                            binding: 0,
                            key: r.get(2)?,
                            doc: r.get(3)?,
                            delete: r.get::<_, i64>(4)? != 0,
                        },
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };

        write_rows(&txn, &rows)?;

        if idempotent {
            txn.execute(
                "DELETE FROM _flow_staged WHERE shard = ?1 AND txn = ?2",
                (shard, txn_id),
            )?;
        }
        txn.commit()?;

        Ok(!rows.is_empty())
    }

    /// Discard staged rows of transactions after `txn_id` — work of a
    /// transaction that never committed to the recovery log, and so must never
    /// become visible.
    pub fn discard_staged_after(&self, shard: u32, txn_id: i64) -> anyhow::Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM _flow_staged WHERE shard = ?1 AND txn > ?2",
            (shard, txn_id),
        )?;
        Ok(n)
    }

    /// Shards holding staged rows. `Apply` has no range of its own, so this is
    /// how it finds the pending work it must drain.
    pub fn staged_shard_keys(&self) -> anyhow::Result<Vec<u32>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT shard FROM _flow_staged ORDER BY shard")?;
        let shards = stmt
            .query_map((), |r| r.get::<_, u32>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(shards)
    }

    /// Every transaction with staged rows for this shard, oldest first.
    pub fn staged_txns(&self, shard: u32) -> anyhow::Result<Vec<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT txn FROM _flow_staged WHERE shard = ?1 ORDER BY txn")?;
        let txns = stmt
            .query_map((shard,), |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(txns)
    }

    /// Append `rows` to their (delta) tables and advance the destination's
    /// committed append count, in one transaction. The document-counter class's
    /// `Store` path: rows become visible immediately, and the count is the
    /// destination's own record of how far it got.
    pub fn append_counted(&self, shard: u32, rows: &[(Table, Row)]) -> anyhow::Result<()> {
        let txn = self.conn.unchecked_transaction()?;

        write_rows(&txn, rows)?;

        let mut stmt = txn.prepare(
            "INSERT INTO _flow_counter (shard, tbl, appended) VALUES (?1, ?2, 1)
             ON CONFLICT (shard, tbl) DO UPDATE SET appended = appended + 1",
        )?;
        for (table, _) in rows {
            stmt.execute((shard, &table.name))?;
        }
        drop(stmt);

        txn.commit()?;
        Ok(())
    }

    /// The destination's committed append count for a resource — the "committed
    /// offset token" the document-counter class resumes from.
    pub fn appended(&self, shard: u32, table: &str) -> anyhow::Result<i64> {
        let n = self
            .conn
            .query_row(
                "SELECT appended FROM _flow_counter WHERE shard = ?1 AND tbl = ?2",
                (shard, table),
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(n)
    }

    pub fn reset_appended(&self, shard: u32) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM _flow_counter WHERE shard = ?1", (shard,))?;
        Ok(())
    }

    pub fn record_applied_spec(&self, version: &str, description: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO _flow_spec (version, applied) VALUES (?1, ?2)",
            (version, description),
        )?;
        Ok(())
    }

    /// Every row of a materialized resource, in delivery order for a delta
    /// binding and key order for a standard one. This is what the `read`
    /// subcommand emits and the invariant checkers consume.
    pub fn read_all(&self, table: &Table) -> anyhow::Result<Vec<String>> {
        let order = if table.delta { "ord" } else { "key" };
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT doc FROM \"{}\" ORDER BY {order}", table.name))
            .with_context(|| format!("reading table {}", table.name))?;

        let docs = stmt
            .query_map((), |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(docs)
    }
}

/// Upsert or append `rows`, per each row's table shape.
fn write_rows(txn: &rusqlite::Transaction<'_>, rows: &[(Table, Row)]) -> anyhow::Result<()> {
    for (table, row) in rows {
        if row.delete && !table.delta {
            txn.execute(
                &format!("DELETE FROM \"{}\" WHERE key = ?1", table.name),
                (&row.key,),
            )?;
        } else if table.delta {
            txn.execute(
                &format!("INSERT INTO \"{}\" (key, doc) VALUES (?1, ?2)", table.name),
                (&row.key, &row.doc),
            )?;
        } else {
            txn.execute(
                &format!(
                    "INSERT INTO \"{}\" (key, doc) VALUES (?1, ?2)
                     ON CONFLICT (key) DO UPDATE SET doc = ?2",
                    table.name
                ),
                (&row.key, &row.doc),
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    /// A smoke test of the SQL itself.
    ///
    /// The design deliberately puts one seam at the scenario runner, and this is a
    /// narrow exception to that: every statement here is either valid SQL or a loud
    /// error, and the feedback loop for a loud error is ten minutes with a live
    /// stack versus a second without one. It does not assert anything about
    /// consistency — that is the scenarios' job.
    #[test]
    fn a_destination_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let standard = Table {
            name: "accounts".to_string(),
            delta: false,
        };
        let delta = Table {
            name: "events".to_string(),
            delta: true,
        };
        store.ensure_table(&standard).unwrap();
        store.ensure_table(&delta).unwrap();

        // A fresh range takes nonce 1 and has no checkpoint to resume from.
        let (nonce, checkpoint) = store.fence(0, u32::MAX).unwrap();
        assert_eq!((nonce, checkpoint), (1, None));

        let row = |key: &str, doc: &str| Row {
            binding: 0,
            key: key.to_string(),
            doc: doc.to_string(),
            delete: false,
        };

        store
            .commit(
                0,
                u32::MAX,
                nonce,
                Some(b"checkpoint"),
                &[
                    (standard.clone(), row("[1]", r#"{"id":1}"#)),
                    (delta.clone(), row("[1,0]", r#"{"id":1,"seq":0}"#)),
                ],
                true,
            )
            .unwrap();

        assert_eq!(store.load(&standard, "[1]").unwrap().as_deref(), Some(r#"{"id":1}"#));
        assert_eq!(store.read_all(&delta).unwrap().len(), 1);

        // A standard binding upserts, so the same key does not accumulate rows.
        store
            .commit(
                0,
                u32::MAX,
                nonce,
                None,
                &[(standard.clone(), row("[1]", r#"{"id":1,"v":2}"#))],
                true,
            )
            .unwrap();
        assert_eq!(store.read_all(&standard).unwrap().len(), 1);

        // Re-opening the range raises the nonce and hands back the checkpoint, and
        // the stale nonce can no longer commit.
        let (next, checkpoint) = store.fence(0, u32::MAX).unwrap();
        assert_eq!(next, 2);
        assert_eq!(checkpoint.as_deref(), Some(b"checkpoint".as_slice()));

        let fenced = store.commit(0, u32::MAX, nonce, None, &[], true);
        assert!(fenced.is_err(), "a stale nonce must be refused");

        // Staging is invisible until applied, and applying twice is a no-op.
        store
            .stage(0, 7, &[(delta.clone(), row("[1,1]", r#"{"id":1,"seq":1}"#))])
            .unwrap();
        assert_eq!(store.read_all(&delta).unwrap().len(), 1);
        assert_eq!(store.staged_txns(0).unwrap(), vec![7]);

        assert!(store.apply_staged(0, 7, true).unwrap());
        assert_eq!(store.read_all(&delta).unwrap().len(), 2);
        assert!(!store.apply_staged(0, 7, true).unwrap());
        assert_eq!(store.read_all(&delta).unwrap().len(), 2);

        // The append counter is the destination's own record of how far it got.
        store
            .append_counted(0, &[(delta.clone(), row("[1,2]", r#"{"id":1,"seq":2}"#))])
            .unwrap();
        assert_eq!(store.appended(0, &delta.name).unwrap(), 1);
    }

    /// A split subdivides a range that has never been opened, so the child has to
    /// inherit its parent's checkpoint — and the parent must come away fenced.
    #[test]
    fn a_split_child_inherits_the_checkpoint_and_fences_its_parent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let (parent_nonce, _) = store.fence(0, u32::MAX).unwrap();
        store
            .commit(0, u32::MAX, parent_nonce, Some(b"parent"), &[], true)
            .unwrap();

        let (low, checkpoint) = store.fence(0, u32::MAX / 2).unwrap();
        assert_eq!(checkpoint.as_deref(), Some(b"parent".as_slice()));
        assert!(low > parent_nonce);

        let (high, checkpoint) = store.fence(u32::MAX / 2 + 1, u32::MAX).unwrap();
        assert_eq!(checkpoint.as_deref(), Some(b"parent".as_slice()));

        // The parent cannot commit behind either child.
        assert!(
            store
                .commit(0, u32::MAX, parent_nonce, None, &[], true)
                .is_err(),
            "the parent must be fenced by its children"
        );
        assert!(high >= low);
    }
}
