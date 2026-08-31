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

impl Table {
    /// The table as a quoted SQL identifier, with embedded quotes doubled.
    fn ident(&self) -> String {
        format!("\"{}\"", self.name.replace('"', "\"\""))
    }
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

        // Switch journal mode to WAL if needed. Setting it unconditionally would fail with
        // "database is locked" whenever a sibling shard has the destination open, because
        // changing the mode takes a brief exclusive lock that SQLite refuses outright rather
        // than waiting on the busy handler. The mode is a durable property of the file, so
        // only the first opener has to set it.
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
    ///
    /// Run for *every* class, though only `remoteAuthoritative` reads or writes the fence
    /// checkpoint: one `Open` path means a scenario cannot pass merely because its class
    /// skipped the bookkeeping, and the checkpoint inheritance above is wanted by every
    /// class that resumes.
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

        // Detect a join: rows whose range sits strictly inside ours belong to a split's
        // children, so if any exist, this session is the survivor of a join. The row stored
        // under our own range is the pre-split parent's, and its checkpoint is older than
        // what the children have committed since — so the survivor must not resume from it,
        // and instead adopts no checkpoint at all, falling back to the recovery log.
        //
        // The children's rows are deleted below: they belong to shards that no longer
        // exist, and their presence is the join signal — left in place, every future open
        // of this range would look like a join again and refuse its own, by then current,
        // checkpoint.
        let children: Vec<_> = overlapping
            .iter()
            .filter(|(kb, ke, _, _)| {
                *kb >= key_begin && *ke <= key_end && (*kb > key_begin || *ke < key_end)
            })
            .map(|(kb, ke, _, _)| (*kb, *ke))
            .collect();
        let joined = !children.is_empty();

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

        // Absorbed ranges no longer exist, so their rows are retired now that this session
        // has adopted nothing from them. Done after the nonce bump above, so a zombie of a
        // departed child is still fenced off by it.
        for (kb, ke) in &children {
            txn.execute(
                "DELETE FROM _flow_fence WHERE key_begin = ?1 AND key_end = ?2",
                (kb, ke),
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
                "CREATE TABLE IF NOT EXISTS {} (
                     ord INTEGER PRIMARY KEY AUTOINCREMENT,
                     key TEXT NOT NULL,
                     doc TEXT NOT NULL)",
                table.ident()
            )
        } else {
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                     key TEXT PRIMARY KEY,
                     doc TEXT NOT NULL)",
                table.ident()
            )
        };
        self.conn
            .execute_batch(&ddl)
            .with_context(|| format!("creating table {}", table.name))?;
        Ok(())
    }

    /// Read a key's current document, from applied state only.
    pub fn load(&self, table: &Table, key: &str) -> anyhow::Result<Option<String>> {
        let doc = self
            .conn
            .query_row(
                &format!("SELECT doc FROM {} WHERE key = ?1", table.ident()),
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

        // The fence is checked *by* the update that writes the checkpoint, not by a read
        // beside it: one statement whose `WHERE nonce = ?` makes ownership a condition of
        // the write, and whose affected-row count is the verdict — the same shape as
        // `materialize-postgres`.
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
        let ident = table.ident();
        let mut queries = Vec::new();

        if table.delta {
            // Appended unconditionally, with no cross-batch deduplication:
            // `materialize-databricks` dedupes at *file* granularity with fresh-UUID names,
            // so the same row re-staged in a new file loads again, and absorbing it would
            // make this connector more forgiving than the one it models. Same-batch
            // idempotency comes from retiring the batch below.
            queries.push(format!(
                "INSERT INTO {ident} (key, doc)
                 SELECT s.key, s.doc FROM _flow_staged s
                 WHERE s.batch = {b} AND s.tbl = {t}
                 ORDER BY s.ord;"
            ));
        } else {
            queries.push(format!(
                "DELETE FROM {ident} WHERE key IN (
                     SELECT key FROM _flow_staged
                     WHERE batch = {b} AND tbl = {t} AND del != 0);"
            ));
            // An absolute upsert, so applying it twice writes the same value — the
            // reason a merged binding needs no ledger.
            queries.push(format!(
                "INSERT INTO {ident} (key, doc)
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
                "SELECT doc FROM {} ORDER BY {order}",
                table.ident()
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
                &format!("DELETE FROM {} WHERE key = ?1", table.ident()),
                (&row.key,),
            )?;
        } else if table.delta {
            txn.execute(
                &format!("INSERT INTO {} (key, doc) VALUES (?1, ?2)", table.ident()),
                (&row.key, &row.doc),
            )?;
        } else {
            txn.execute(
                &format!(
                    "INSERT INTO {} (key, doc) VALUES (?1, ?2)
                     ON CONFLICT (key) DO UPDATE SET doc = ?2",
                    table.ident()
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
    /// repeat a no-op — and what the `non-idempotent-acknowledge` defect breaks by not retiring.
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

    /// A row re-staged under a *new* batch is appended again, which is what a real
    /// destination does — see the comment in `apply_statements`.
    #[test]
    fn applying_the_same_rows_under_a_new_batch_appends_again() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        let table = Table {
            name: "events".to_string(),
            delta: true,
        };
        store.ensure_table(&table).unwrap();

        let row = Row {
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
            2,
            "a new staged batch appends, as a real destination's would",
        );
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
    /// exists — that row belongs to the pre-split parent, and the runtime rejects its stale
    /// checkpoint.
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

    /// ...and only that first open. A later session over the same range adopts its own
    /// checkpoint again: the join signal is the presence of narrower rows, which are
    /// deleted once the survivor has adopted nothing from them.
    #[test]
    fn a_later_session_after_a_join_adopts_its_own_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("d.sqlite")).unwrap();

        // A split's two children each fence their own half.
        store.fence(0, u32::MAX / 2).unwrap();
        store.fence(u32::MAX / 2 + 1, u32::MAX).unwrap();

        // The survivor of the join adopts nothing, as the test above asserts.
        let (nonce, checkpoint) = store.fence(0, u32::MAX).unwrap();
        assert!(checkpoint.is_none(), "the join survivor adopts nothing");

        // It then commits, writing a checkpoint into its own row.
        store
            .commit(0, u32::MAX, nonce, Some(b"post-join"), &[], true)
            .unwrap();

        // A later session over the same range — a plain crash-restart — must resume from it.
        let (_, checkpoint) = store.fence(0, u32::MAX).unwrap();
        assert_eq!(
            checkpoint.as_deref(),
            Some(b"post-join".as_slice()),
            "a join must not permanently disable checkpoint recovery for the range",
        );
    }
}
