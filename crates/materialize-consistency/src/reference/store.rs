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
        conn.busy_timeout(std::time::Duration::from_secs(30))
            .context("setting the busy timeout")?;

        // Switch to WAL only if the file is not already in it.
        //
        // Changing journal mode needs a brief exclusive lock, and SQLite fails that
        // outright rather than consulting the busy handler — so an unconditional
        // `PRAGMA journal_mode = WAL` dies with "database is locked" whenever a
        // sibling shard has the destination open. That took down every shard-split
        // scenario: the second child of a split failed during `Open`, and the leader
        // reported it two layers up as an unexpected EOF from its fan-in.
        //
        // Journal mode is a durable property of the file, so the first opener sets it
        // — uncontended, because it is also the one creating the file — and everyone
        // after reads `wal` and leaves it alone. Reading takes no exclusive lock.
        //
        // Both statements go through `query_row` rather than the batch below because
        // they *return* the mode, which `execute_batch` refuses.
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .context("reading the journal mode")?;

        if !mode.eq_ignore_ascii_case("wal") {
            conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
                .context("enabling WAL journaling")?;
        }

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
                 ord       INTEGER PRIMARY KEY AUTOINCREMENT,
                 txn       INTEGER NOT NULL,
                 shard     INTEGER NOT NULL,
                 shard_end INTEGER NOT NULL,
                 tbl       TEXT    NOT NULL,
                 delta     INTEGER NOT NULL,
                 key       TEXT    NOT NULL,
                 doc       TEXT    NOT NULL,
                 del       INTEGER NOT NULL
             );

             CREATE TABLE IF NOT EXISTS _flow_applied_txn (
                 shard     INTEGER NOT NULL,
                 shard_end INTEGER NOT NULL,
                 txn       INTEGER NOT NULL,
                 PRIMARY KEY (shard, shard_end, txn)
             );

             CREATE TABLE IF NOT EXISTS _flow_counter (
                 shard     INTEGER NOT NULL,
                 shard_end INTEGER NOT NULL,
                 tbl       TEXT    NOT NULL,
                 appended  INTEGER NOT NULL,
                 PRIMARY KEY (shard, shard_end, tbl)
             );

             CREATE TABLE IF NOT EXISTS _flow_spec (
                 version TEXT PRIMARY KEY,
                 applied TEXT NOT NULL
             );",
        )
        .context("initializing destination bookkeeping")?;

        Ok(Self { conn })
    }

    /// Begin a write transaction, taking the write lock up front.
    ///
    /// `BEGIN IMMEDIATE`, not the default `DEFERRED`, and that distinction is the
    /// difference between the split scenarios working and not. Every transaction here
    /// reads and then writes; a deferred transaction takes only a read lock at the
    /// SELECT and tries to upgrade at the UPDATE — and in WAL mode, upgrading after
    /// another connection has written returns SQLITE_BUSY_SNAPSHOT *immediately*,
    /// without consulting the busy handler. So the busy timeout is no help and the
    /// loser of the race just fails: the second child of a split died fencing during
    /// `Open`, which the leader reported as an unexpected EOF from its fan-in.
    ///
    /// Taking the write lock at `BEGIN` means contention waits out the busy timeout
    /// instead, which is what a destination shared by two shards needs.
    fn write_txn(&self) -> rusqlite::Result<rusqlite::Transaction<'_>> {
        rusqlite::Transaction::new_unchecked(&self.conn, rusqlite::TransactionBehavior::Immediate)
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
        let txn = self.write_txn()?;

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

    /// Load a key, honouring staged writes that have not yet been applied.
    ///
    /// A connector must answer `Load` consistently with everything it has been asked
    /// to `Store` in a committed transaction — whether or not it has physically
    /// applied it yet. Reading the destination table alone is *not* sufficient for a
    /// class that stages during `Store` and applies during `Acknowledge`: between
    /// those two points the rows are durable but invisible, and a `Load` answered
    /// from the table returns a document missing that transaction's contribution.
    /// The runtime then reduces from a stale base and stores an incorrect reduction.
    ///
    /// This is only reachable after a shard reconfiguration, which is why it hid for
    /// so long: the runtime re-uses documents it has cached from prior transactions,
    /// so a long-lived session rarely issues a real `Load` for a key it just stored.
    /// A split gives its children cold caches, they issue real Loads, and any that
    /// lands inside the staged-but-unapplied window gets a stale answer.
    ///
    /// Staging is consulted for every range that *contains* this session's, which is
    /// this session's own range and its ancestors'.
    ///
    /// Its own, because a `Load` must reflect writes it has staged but not applied.
    /// Its ancestors', because a split child inherits keys whose staged rows still sit
    /// under the parent's wider range — and missing those is what made
    /// `split-during-commit` fail: the delta binding, which needs no `Load`,
    /// accumulated exactly to the expectation while the standard binding's reduced
    /// value was wrong with its sequence fully up to date. Only a stale `Load` base
    /// can do that.
    ///
    /// Containment rather than "every shard" is what keeps the `ignore-key-range`
    /// defect catchable: siblings never contain one another, so they still cannot see
    /// each other's writes.
    pub fn load(
        &self,
        key_begin: u32,
        key_end: u32,
        table: &Table,
        key: &str,
    ) -> anyhow::Result<Option<String>> {
        let staged: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT doc, del FROM _flow_staged
                 WHERE shard <= ?1 AND shard_end >= ?2 AND tbl = ?3 AND key = ?4
                 ORDER BY ord DESC LIMIT 1",
                (key_begin, key_end, &table.name, key),
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .context("loading from staged writes")?;

        if let Some((doc, deleted)) = staged {
            return Ok(if deleted != 0 { None } else { Some(doc) });
        }

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
        let txn = self.write_txn()?;

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
    pub fn stage(
        &self,
        shard: u32,
        shard_end: u32,
        txn_id: i64,
        rows: &[(Table, Row)],
    ) -> anyhow::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut stmt = txn.prepare(
                "INSERT INTO _flow_staged (txn, shard, shard_end, tbl, delta, key, doc, del)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for (table, row) in rows {
                stmt.execute((
                    txn_id,
                    shard,
                    shard_end,
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
    pub fn apply_staged(
        &self,
        key_begin: u32,
        key_end: u32,
        txn_id: i64,
        idempotent: bool,
    ) -> anyhow::Result<bool> {
        let txn = self.write_txn()?;

        if idempotent {
            let claimed = txn.execute(
                "INSERT OR IGNORE INTO _flow_applied_txn (shard, shard_end, txn)
                 VALUES (?1, ?2, ?3)",
                (key_begin, key_end, txn_id),
            )?;
            if claimed == 0 {
                txn.commit()?;
                return Ok(false);
            }
        }

        let rows = {
            let mut stmt = txn.prepare(
                "SELECT tbl, delta, key, doc, del FROM _flow_staged
                 WHERE shard = ?1 AND shard_end = ?2 AND txn = ?3 ORDER BY ord",
            )?;
            let rows = stmt
                .query_map((key_begin, key_end, txn_id), |r| {
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
                "DELETE FROM _flow_staged
                 WHERE shard = ?1 AND shard_end = ?2 AND txn = ?3",
                (key_begin, key_end, txn_id),
            )?;
        }
        txn.commit()?;

        Ok(!rows.is_empty())
    }

    /// Discard staged rows of transactions after `txn_id` — work of a
    /// transaction that never committed to the recovery log, and so must never
    /// become visible.
    pub fn discard_staged_after(
        &self,
        key_begin: u32,
        key_end: u32,
        txn_id: i64,
    ) -> anyhow::Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM _flow_staged
             WHERE shard = ?1 AND shard_end = ?2 AND txn > ?3",
            (key_begin, key_end, txn_id),
        )?;
        Ok(n)
    }

    /// Shards holding staged rows. `Apply` has no range of its own, so this is
    /// how it finds the pending work it must drain.
    pub fn staged_shard_keys(&self) -> anyhow::Result<Vec<(u32, u32)>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT shard, shard_end FROM _flow_staged ORDER BY shard, shard_end",
        )?;
        let ranges = stmt
            .query_map((), |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ranges)
    }

    /// Every transaction with staged rows for this shard, oldest first.
    pub fn staged_txns(&self, key_begin: u32, key_end: u32) -> anyhow::Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT txn FROM _flow_staged
             WHERE shard = ?1 AND shard_end = ?2 ORDER BY txn",
        )?;
        let txns = stmt
            .query_map((key_begin, key_end), |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(txns)
    }

    /// Append `rows` to their (delta) tables and advance the destination's
    /// committed append count, in one transaction. The document-counter class's
    /// `Store` path: rows become visible immediately, and the count is the
    /// destination's own record of how far it got.
    pub fn append_counted(
        &self,
        key_begin: u32,
        key_end: u32,
        rows: &[(Table, Row)],
    ) -> anyhow::Result<()> {
        let txn = self.write_txn()?;

        write_rows(&txn, rows)?;

        let mut stmt = txn.prepare(
            "INSERT INTO _flow_counter (shard, shard_end, tbl, appended) VALUES (?1, ?2, ?3, 1)
             ON CONFLICT (shard, shard_end, tbl) DO UPDATE SET appended = appended + 1",
        )?;
        for (table, _) in rows {
            stmt.execute((key_begin, key_end, &table.name))?;
        }
        drop(stmt);

        txn.commit()?;
        Ok(())
    }

    /// The destination's committed append count for a resource — the "committed
    /// offset token" the document-counter class resumes from.
    pub fn appended(&self, key_begin: u32, key_end: u32, table: &str) -> anyhow::Result<i64> {
        let n = self
            .conn
            .query_row(
                "SELECT appended FROM _flow_counter
                 WHERE shard = ?1 AND shard_end = ?2 AND tbl = ?3",
                (key_begin, key_end, table),
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(n)
    }

    pub fn reset_appended(&self, key_begin: u32, key_end: u32) -> anyhow::Result<()> {
        self.conn.execute(
            "DELETE FROM _flow_counter WHERE shard = ?1 AND shard_end = ?2",
            (key_begin, key_end),
        )?;
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
            .prepare(&format!(
                "SELECT doc FROM \"{}\" ORDER BY {order}",
                table.name
            ))
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

        assert_eq!(
            store
                .load(0, u32::MAX, &standard, "[1]")
                .unwrap()
                .as_deref(),
            Some(r#"{"id":1}"#)
        );
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
            .stage(
                0,
                u32::MAX,
                7,
                &[(delta.clone(), row("[1,1]", r#"{"id":1,"seq":1}"#))],
            )
            .unwrap();
        assert_eq!(store.read_all(&delta).unwrap().len(), 1);
        assert_eq!(store.staged_txns(0, u32::MAX).unwrap(), vec![7]);

        assert!(store.apply_staged(0, u32::MAX, 7, true).unwrap());
        assert_eq!(store.read_all(&delta).unwrap().len(), 2);
        assert!(!store.apply_staged(0, u32::MAX, 7, true).unwrap());
        assert_eq!(store.read_all(&delta).unwrap().len(), 2);

        // Several transactions can be staged and committed without being applied — a
        // session fenced mid-flight leaves exactly that — so recovery has to see all of
        // them, not just the newest. Applying only the newest strands the others: the
        // discard path never reclaims them either, because it only removes transactions
        // *after* the committed one.
        store
            .stage(
                0,
                u32::MAX,
                8,
                &[(delta.clone(), row("[2,0]", r#"{"id":2}"#))],
            )
            .unwrap();
        store
            .stage(
                0,
                u32::MAX,
                9,
                &[(delta.clone(), row("[3,0]", r#"{"id":3}"#))],
            )
            .unwrap();
        assert_eq!(store.staged_txns(0, u32::MAX).unwrap(), vec![8, 9]);

        let before = store.read_all(&delta).unwrap().len();
        for txn in store.staged_txns(0, u32::MAX).unwrap() {
            store.apply_staged(0, u32::MAX, txn, true).unwrap();
        }
        assert_eq!(
            store.read_all(&delta).unwrap().len(),
            before + 2,
            "every staged transaction must be applied, not only the newest",
        );
        assert!(store.staged_txns(0, u32::MAX).unwrap().is_empty());

        // The append counter is the destination's own record of how far it got.
        store
            .append_counted(
                0,
                u32::MAX,
                &[(delta.clone(), row("[1,2]", r#"{"id":1,"seq":2}"#))],
            )
            .unwrap();
        assert_eq!(store.appended(0, u32::MAX, &delta.name).unwrap(), 1);
    }

    /// The root cause of `split-during-commit`'s intermittent failures: a `Load` must
    /// see writes that are staged but not yet applied.
    ///
    /// Between `Store` and `Acknowledge` a post-commit-apply connector's rows are
    /// durable and invisible. A `Load` answered from the destination table alone
    /// returns a document missing that transaction's contribution, the runtime
    /// reduces from that stale base, and the reduction it stores is wrong by exactly
    /// one transaction — under-counting or over-counting according to the sign of the
    /// delta, which is what made the symptom look like a torn reduction.
    #[test]
    fn a_load_sees_staged_writes_before_they_are_applied() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let table = Table {
            name: "accounts".to_string(),
            delta: false,
        };
        store.ensure_table(&table).unwrap();

        let row = |doc: &str| Row {
            binding: 0,
            key: "[1]".to_string(),
            doc: doc.to_string(),
            delete: false,
        };

        // Applied: visible the ordinary way.
        store
            .stage(0, u32::MAX, 1, &[(table.clone(), row(r#"{"balance":10}"#))])
            .unwrap();
        assert!(store.apply_staged(0, u32::MAX, 1, true).unwrap());
        assert_eq!(
            store.load(0, u32::MAX, &table, "[1]").unwrap().as_deref(),
            Some(r#"{"balance":10}"#),
        );

        // Staged and *not* applied: still the answer a Load must give, because the
        // connector has been asked to store it.
        store
            .stage(0, u32::MAX, 2, &[(table.clone(), row(r#"{"balance":25}"#))])
            .unwrap();
        assert_eq!(
            store.load(0, u32::MAX, &table, "[1]").unwrap().as_deref(),
            Some(r#"{"balance":25}"#),
            "a Load must reflect staged-but-unapplied writes",
        );

        // An *ancestor's* staging is visible to a split child: the child inherits keys
        // whose staged rows still sit under the parent's wider range.
        assert_eq!(
            store
                .load(0x8000_0000, 0xffff_ffff, &table, "[1]")
                .unwrap()
                .as_deref(),
            Some(r#"{"balance":25}"#),
            "an ancestor's staging must be visible to a split child",
        );

        // A *sibling's* is not: its range does not contain the loader's. This is what
        // keeps the `ignore-key-range` defect catchable, and it is why the lookup is
        // by containment rather than simply "every shard".
        store
            .stage(
                0x8000_0000,
                0xffff_ffff,
                1,
                &[(table.clone(), row(r#"{"balance":40}"#))],
            )
            .unwrap();
        assert_eq!(
            store
                .load(0, 0x7fff_ffff, &table, "[1]")
                .unwrap()
                .as_deref(),
            Some(r#"{"balance":25}"#),
            "a sibling's staging must not be visible",
        );
        assert_eq!(
            store
                .load(0x8000_0000, 0xffff_ffff, &table, "[1]")
                .unwrap()
                .as_deref(),
            Some(r#"{"balance":40}"#),
            "a shard sees its own staging, newest first",
        );

        // A staged deletion is a tombstone, not a fall-through to the stale table row.
        store
            .stage(
                0,
                u32::MAX,
                3,
                &[(
                    table.clone(),
                    Row {
                        binding: 0,
                        key: "[1]".to_string(),
                        doc: String::new(),
                        delete: true,
                    },
                )],
            )
            .unwrap();
        assert_eq!(store.load(0, u32::MAX, &table, "[1]").unwrap(), None);
    }

    /// Staged work is identified by its whole key range, so a split child can tell an
    /// ancestor's leftovers from a live sibling's in-flight work.
    ///
    /// Keying on `key_begin` alone cannot: after a two-way split the low child shares
    /// its begin with the departed parent, so "shard 0's staging" names both the
    /// ancestor's and the sibling's. Acting on that ambiguity means discarding a
    /// sibling's uncommitted transaction — the exact loss this suite exists to catch.
    #[test]
    fn staged_work_distinguishes_an_ancestor_from_a_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let table = Table {
            name: "events".to_string(),
            delta: true,
        };
        store.ensure_table(&table).unwrap();

        let row = |key: &str| Row {
            binding: 0,
            key: key.to_string(),
            doc: format!(r#"{{"k":"{key}"}}"#),
            delete: false,
        };

        // The parent staged a transaction and departed. Both children of the split
        // share nothing with each other; the low child shares `key_begin` with the
        // parent.
        const PARENT: (u32, u32) = (0, u32::MAX);
        const LOW: (u32, u32) = (0, 0x7fff_ffff);
        const HIGH: (u32, u32) = (0x8000_0000, u32::MAX);

        store
            .stage(PARENT.0, PARENT.1, 4, &[(table.clone(), row("[1]"))])
            .unwrap();
        store
            .stage(LOW.0, LOW.1, 1, &[(table.clone(), row("[2]"))])
            .unwrap();

        // Each range sees only its own staging, even where begins coincide.
        assert_eq!(store.staged_txns(PARENT.0, PARENT.1).unwrap(), vec![4]);
        assert_eq!(store.staged_txns(LOW.0, LOW.1).unwrap(), vec![1]);
        assert!(store.staged_txns(HIGH.0, HIGH.1).unwrap().is_empty());

        // The high child discarding its ancestor's uncommitted work must not touch the
        // low child's, though the two share a `key_begin`.
        store.discard_staged_after(PARENT.0, PARENT.1, 3).unwrap();
        assert!(store.staged_txns(PARENT.0, PARENT.1).unwrap().is_empty());
        assert_eq!(
            store.staged_txns(LOW.0, LOW.1).unwrap(),
            vec![1],
            "a sibling's in-flight staging must survive",
        );

        // And an applied transaction is claimed per range, so the same number under a
        // different range is a different transaction.
        store
            .stage(HIGH.0, HIGH.1, 1, &[(table.clone(), row("[3]"))])
            .unwrap();
        assert!(store.apply_staged(LOW.0, LOW.1, 1, true).unwrap());
        assert!(
            store.apply_staged(HIGH.0, HIGH.1, 1, true).unwrap(),
            "txn 1 of one range must not be mistaken for txn 1 of another",
        );
        assert_eq!(store.read_all(&table).unwrap().len(), 2);
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
