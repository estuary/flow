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
#[derive(Clone)]
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
                 ord   INTEGER PRIMARY KEY AUTOINCREMENT,
                 batch TEXT    NOT NULL,
                 tbl   TEXT    NOT NULL,
                 key   TEXT    NOT NULL,
                 doc   TEXT    NOT NULL,
                 del   INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS _flow_staged_batch ON _flow_staged (batch, tbl);

             CREATE TABLE IF NOT EXISTS _flow_applied_row (
                 tbl   TEXT NOT NULL,
                 key   TEXT NOT NULL,
                 doc   TEXT NOT NULL,
                 batch TEXT NOT NULL,
                 PRIMARY KEY (tbl, key, doc)
             );

             CREATE TABLE IF NOT EXISTS _flow_suppressed (
                 tbl  TEXT PRIMARY KEY,
                 rows INTEGER NOT NULL
             );

             CREATE TABLE IF NOT EXISTS _flow_counter (
                 shard     INTEGER NOT NULL,
                 shard_end INTEGER NOT NULL,
                 tbl       TEXT    NOT NULL,
                 appended  INTEGER NOT NULL,
                 PRIMARY KEY (shard, shard_end, tbl)
             );
",
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

    /// Claim `[key_begin, key_end]` — inclusive, as Flow's ranges are — returning the
    /// nonce this session holds and
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

        // The survivor of a join must adopt nothing, and this is not a nicety: its
        // range key was last used by the *pre-split parent*, whose row is still here
        // with a checkpoint from before the split. Adopting it hands the runtime a
        // close-clock from a superseded history, which recovery rejects outright —
        // `connector_checkpoint has clock ... which doesn't match Recover's
        // committed_close or hinted_close` — and the shard then crash-loops rather
        // than resuming. Falling back to the recovery log is the correct resume point,
        // because two ranges collapsing into one leaves no single range that contained
        // the result.
        //
        // A strictly narrower overlapping row is what identifies a join: those are the
        // split's children, whose work is more recent than anything filed under this
        // shard's own range.
        let joined = overlapping.iter().any(|(kb, ke, _, _)| {
            *kb >= key_begin && *ke <= key_end && (*kb > key_begin || *ke < key_end)
        });

        // A split's child, by contrast, inherits from the narrowest range that
        // strictly contains it: that ancestor's checkpoint *is* its resume point.
        let checkpoint = (!joined)
            .then(|| {
                overlapping
                    .iter()
                    .filter(|(kb, ke, _, _)| *kb <= key_begin && *ke >= key_end)
                    .min_by_key(|(kb, ke, _, _)| (*ke as u64) - (*kb as u64))
                    .and_then(|(_, _, _, cp)| cp.clone())
            })
            .flatten();

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
    /// Read a key's current document, from applied state only.
    ///
    /// A connector does not need to consult its own pending work here, because the
    /// protocol lets it wait instead: `LoadIterator::WaitForAcknowledged` blocks until the
    /// previous transaction has been fully acknowledged, and only then may loads be issued,
    /// at which point the destination already holds everything committed. This connector
    /// processes requests in order and applies at `Acknowledge`, so it has that property
    /// without an explicit wait — and `Open` applies anything left pending by a previous
    /// session before serving a request.
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
        let txn = self.write_txn()?;

        write_rows(&txn, rows)?;

        // The fence is checked *by* the update that writes the checkpoint, not by a
        // read beside it — one statement whose `WHERE nonce = ?` makes ownership a
        // condition of the write, and whose affected-row count is the verdict. This is
        // what `materialize-postgres` does, queueing the fence update as the last
        // statement of the same transaction and treating anything other than one
        // affected row as having been fenced off.
        //
        // A read followed by a write would be two operations with a window between
        // them: correct only because SQLite's `BEGIN IMMEDIATE` happens to serialise
        // writers, and wrong on any store that does not.
        let updated = txn.execute(
            "UPDATE _flow_fence SET checkpoint = COALESCE(?4, checkpoint)
             WHERE key_begin = ?1 AND key_end = ?2 AND (?3 IS NULL OR nonce = ?3)",
            (key_begin, key_end, check_fence.then_some(nonce), checkpoint),
        )?;

        anyhow::ensure!(
            updated == 1,
            "fenced off: no fence row for [{key_begin:08x}, {key_end:08x}] still holds \
             nonce {nonce}, so another session has claimed this range",
        );

        txn.commit()?;
        Ok(())
    }

    /// Durably record `rows` under `batch` without making them visible. The
    /// post-commit-apply class's `Store` path.
    ///
    /// The analogue of a real connector uploading a staged file: the data lands
    /// somewhere durable and inert, named by something the connector can put in its
    /// checkpoint and act on later.
    pub fn stage(&self, batch: &str, rows: &[(Table, Row)]) -> anyhow::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut stmt = txn.prepare(
                "INSERT INTO _flow_staged (batch, tbl, key, doc, del)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (table, row) in rows {
                stmt.execute((batch, &table.name, &row.key, &row.doc, row.delete as i64))?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Run the statements of one checkpoint entry, as a single transaction.
    ///
    /// The statements come from connector state, which is what makes this the whole of
    /// the apply: nothing here inspects the destination to decide what to do. Running
    /// them together is what makes the entry idempotent — the last statement retires
    /// the batch, so a re-run finds nothing staged and changes nothing.
    pub fn execute(&self, queries: &[String]) -> anyhow::Result<()> {
        let txn = self.write_txn()?;
        for query in queries {
            txn.execute_batch(query)
                .with_context(|| format!("executing staged statement: {query}"))?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Statements which apply `batch` of `table` into the destination, for the
    /// checkpoint to carry.
    ///
    /// Built here and stored in connector state, exactly as `materialize-databricks`
    /// renders `MERGE`/`COPY INTO` at `Store` and keeps them in its checkpoint. The
    /// last statement retires the batch, which is what makes the whole entry
    /// idempotent: re-running it finds nothing staged and does nothing.
    ///
    /// `deduplicate` is off under the `non-idempotent-acknowledge` defect, which leaves
    /// the batch in place so a second run appends it again — the real shape of the bug,
    /// staged files the connector forgets to retire.
    pub fn apply_statements(batch: &str, table: &Table, deduplicate: bool) -> Vec<String> {
        let (b, t) = (quote(batch), quote(&table.name));
        let ident = table.name.replace('"', "\"\"");
        let mut queries = Vec::new();

        if table.delta {
            // A real destination recognises a staged file it has already loaded, so a
            // re-delivered row is absorbed rather than appended twice. SQLite offers no
            // such guarantee, so the ledger stands in for it — claimed atomically, and
            // stamped with the batch that won, so "rows this batch may append" is
            // expressible as a join rather than inferred afterwards.
            queries.push(format!(
                "INSERT OR IGNORE INTO _flow_applied_row (tbl, key, doc, batch)
                 SELECT tbl, key, doc, {b} FROM _flow_staged
                 WHERE batch = {b} AND tbl = {t};"
            ));
            // Count what the ledger refused before the batch is retired: absorbing a
            // re-delivery is correct, but a run that absorbed nothing has demonstrated
            // nothing, and the destination's contents cannot tell the two apart.
            queries.push(format!(
                "INSERT INTO _flow_suppressed (tbl, rows)
                 SELECT s.tbl, COUNT(*) FROM _flow_staged s
                 WHERE s.batch = {b} AND s.tbl = {t} AND NOT EXISTS (
                     SELECT 1 FROM _flow_applied_row r
                     WHERE r.tbl = s.tbl AND r.key = s.key AND r.doc = s.doc
                       AND r.batch = {b})
                 GROUP BY s.tbl
                 ON CONFLICT (tbl) DO UPDATE SET rows = rows + excluded.rows;"
            ));
            queries.push(format!(
                "INSERT INTO \"{ident}\" (key, doc)
                 SELECT s.key, s.doc FROM _flow_staged s
                 WHERE s.batch = {b} AND s.tbl = {t} AND EXISTS (
                     SELECT 1 FROM _flow_applied_row r
                     WHERE r.tbl = s.tbl AND r.key = s.key AND r.doc = s.doc
                       AND r.batch = {b})
                 ORDER BY s.ord;"
            ));
        } else {
            queries.push(format!(
                "DELETE FROM \"{ident}\" WHERE key IN (
                     SELECT key FROM _flow_staged
                     WHERE batch = {b} AND tbl = {t} AND del != 0);"
            ));
            // An absolute upsert, so applying it twice writes the same value — the
            // reason a merged binding needs no ledger.
            queries.push(format!(
                "INSERT INTO \"{ident}\" (key, doc)
                 SELECT key, doc FROM _flow_staged
                 WHERE batch = {b} AND tbl = {t} AND del = 0
                 ORDER BY ord
                 ON CONFLICT (key) DO UPDATE SET doc = excluded.doc;"
            ));
        }

        if deduplicate {
            queries.push(format!(
                "DELETE FROM _flow_staged WHERE batch = {b} AND tbl = {t};"
            ));
        }
        queries
    }

    /// Distinct table names present in a staged batch.
    pub fn staged_tables(&self, batch: &str) -> anyhow::Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT tbl FROM _flow_staged WHERE batch = ?1")?;
        let names = stmt
            .query_map((batch,), |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(names)
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

/// A SQL string literal. The values quoted here are batch ids and table names the
/// connector generated itself, but the statements are persisted in state and later run
/// verbatim, so they are escaped rather than trusted.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

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
            store.load(&standard, "[1]").unwrap().as_deref(),
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

        // Staging is invisible until its statements run, and running them twice is a
        // no-op because the last one retires the batch.
        store
            .stage(
                "batch-7",
                &[(delta.clone(), row("[1,1]", r#"{"id":1,"seq":1}"#))],
            )
            .unwrap();
        assert_eq!(store.read_all(&delta).unwrap().len(), 1);

        let apply_7 = Store::apply_statements("batch-7", &delta, true);
        store.execute(&apply_7).unwrap();
        assert_eq!(store.read_all(&delta).unwrap().len(), 2);
        store.execute(&apply_7).unwrap();
        assert_eq!(store.read_all(&delta).unwrap().len(), 2);

        // Several batches can be staged and unapplied at once — a session that died
        // between committing and acknowledging leaves exactly that — so every entry the
        // checkpoint carries has to be applied, not merely the newest.
        store
            .stage("batch-8", &[(delta.clone(), row("[2,0]", r#"{"id":2}"#))])
            .unwrap();
        store
            .stage("batch-9", &[(delta.clone(), row("[3,0]", r#"{"id":3}"#))])
            .unwrap();

        let before = store.read_all(&delta).unwrap().len();
        for batch in ["batch-8", "batch-9"] {
            store
                .execute(&Store::apply_statements(batch, &delta, true))
                .unwrap();
        }
        assert_eq!(
            store.read_all(&delta).unwrap().len(),
            before + 2,
            "every staged batch must be applied, not only the newest",
        );

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

    /// Applying a checkpoint entry repeatedly must leave the destination exactly as one
    /// application would.
    ///
    /// This is the whole contract of the class: the crashed session is gone rather than
    /// competing, so what matters is that whoever holds the checkpoint can finish its
    /// work — more than once, and from a shard that did not stage it — without inventing
    /// or losing anything. The last statement retires the batch, which is what makes the
    /// repeat a no-op.
    #[test]
    fn applying_staged_work_repeatedly_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let merged = Table {
            name: "accounts".to_string(),
            delta: false,
        };
        let appended = Table {
            name: "events".to_string(),
            delta: true,
        };
        store.ensure_table(&merged).unwrap();
        store.ensure_table(&appended).unwrap();

        let row = |key: &str, doc: &str| Row {
            binding: 0,
            key: key.to_string(),
            doc: doc.to_string(),
            delete: false,
        };

        store
            .stage(
                "batch-1",
                &[
                    (merged.clone(), row("[1]", r#"{"balance":10}"#)),
                    (appended.clone(), row("[1,1]", r#"{"id":1,"seq":1}"#)),
                ],
            )
            .unwrap();

        // Ten applications, from the statements the checkpoint carries.
        for _ in 0..10 {
            for table in [&merged, &appended] {
                store
                    .execute(&Store::apply_statements("batch-1", table, true))
                    .unwrap();
            }
        }

        assert_eq!(
            store.read_all(&merged).unwrap(),
            vec![r#"{"balance":10}"#.to_string()],
            "a merged key holds one value, whatever the apply count",
        );
        assert_eq!(
            store.read_all(&appended).unwrap().len(),
            1,
            "an append appears once, whatever the apply count",
        );
    }

    /// The same document, re-delivered and staged again under a different batch, must not
    /// append twice — and the suppression must be counted.
    ///
    /// Real destinations recognise a staged file they have already loaded, so an
    /// `Acknowledge` is idempotent for append-only bindings too. SQLite offers no such
    /// guarantee, so the row ledger stands in for it, claimed atomically inside the same
    /// transaction as the append.
    #[test]
    fn applying_the_same_rows_from_another_shard_does_not_append_twice() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let table = Table {
            name: "events".to_string(),
            delta: true,
        };
        store.ensure_table(&table).unwrap();

        let row = Row {
            binding: 0,
            key: "[1,7]".to_string(),
            doc: r#"{"id":1,"seq":7}"#.to_string(),
            delete: false,
        };

        // The pre-split shard stages and applies it.
        store
            .stage("parent-1", &[(table.clone(), row.clone())])
            .unwrap();
        store
            .execute(&Store::apply_statements("parent-1", &table, true))
            .unwrap();

        // A split child is re-delivered the same document and stages it as its own.
        store.stage("child-1", &[(table.clone(), row)]).unwrap();
        store
            .execute(&Store::apply_statements("child-1", &table, true))
            .unwrap();

        assert_eq!(
            store.read_all(&table).unwrap().len(),
            1,
            "the destination recognised a row it had already accepted",
        );

        // Suppressing it is what makes the count correct, so it must be visible: a run
        // that absorbed nothing has demonstrated nothing.
        let suppressed: Vec<(String, i64)> = store
            .conn
            .prepare("SELECT tbl, rows FROM _flow_suppressed")
            .unwrap()
            .query_map((), |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(suppressed, vec![("events".to_string(), 1)]);
    }

    /// A `Load` reads applied state, and nothing else.
    ///
    /// The protocol's contract is that a connector *waits* for the previous transaction to
    /// be acknowledged before issuing loads — `LoadIterator::WaitForAcknowledged` — rather
    /// than reading around its own pending work. Staged rows are therefore invisible to a
    /// `Load` until they are applied, and a connector that peeks at them is modelling
    /// something no real one does.
    #[test]
    fn a_load_reads_applied_state_only() {
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

        store
            .stage("batch-1", &[(table.clone(), row(r#"{"balance":10}"#))])
            .unwrap();
        assert_eq!(
            store.load(&table, "[1]").unwrap(),
            None,
            "staged but unapplied work is not yet part of the destination",
        );

        store
            .execute(&Store::apply_statements("batch-1", &table, true))
            .unwrap();
        assert_eq!(
            store.load(&table, "[1]").unwrap().as_deref(),
            Some(r#"{"balance":10}"#),
        );
    }

    /// A join's survivor must adopt no checkpoint, even though a row for its exact range
    /// exists — that row belongs to the pre-split parent and its close-clock comes from a
    /// history the split superseded. The runtime rejects such a checkpoint outright
    /// (`connector_checkpoint has clock ... which doesn't match Recover's committed_close
    /// or hinted_close`) and the shard then crash-loops rather than resuming.
    #[test]
    fn a_join_survivor_adopts_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        // A parent commits, then splits, and each child commits for itself.
        let (parent_nonce, _) = store.fence(0, u32::MAX).unwrap();
        store
            .commit(0, u32::MAX, parent_nonce, Some(b"parent"), &[], true)
            .unwrap();

        let mid = u32::MAX / 2;
        let (low_nonce, _) = store.fence(0, mid).unwrap();
        store
            .commit(0, mid, low_nonce, Some(b"low"), &[], true)
            .unwrap();
        let (high_nonce, _) = store.fence(mid + 1, u32::MAX).unwrap();
        store
            .commit(mid + 1, u32::MAX, high_nonce, Some(b"high"), &[], true)
            .unwrap();

        // The survivor re-widens to the parent's exact range.
        let (survivor, checkpoint) = store.fence(0, u32::MAX).unwrap();
        assert_eq!(
            checkpoint, None,
            "the survivor must not adopt the pre-split parent's stale checkpoint",
        );
        assert!(survivor > high_nonce, "the survivor fences both children");
    }
}
