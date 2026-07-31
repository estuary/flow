//! `CollectionStore`: the harness's in-memory stand-in for collection journals.
//!
//! Each logical partition of a collection is a "journal" — an append-log of
//! committed documents tagged with their synthetic publication clock. Ingest and
//! the `TestPublisher` append; Verify and the shuffle segment feeder read. The
//! store persists across test cases within a run (parity: journals persist in
//! V1; `Reset` clears connector state, not collection data).
//!
//! Offsets are **document counts**, not byte offsets: a journal's write head is
//! the number of documents appended so far. This is the unit of the scheduler's
//! clocks ([`crate::clock::Clock`]) in the harness — the graph maxes ingest /
//! stat write clocks and hands Verify a `(from, to]` window, which maps exactly
//! to a half-open document-index range because every clock value sits on a
//! document boundary.
//!
//! Journal names follow the convention `"{collection}/{partition}"` (a
//! collection always has at least one partition journal); [`journals_of`] and
//! [`write_clock`](CollectionStore::write_clock) select a collection's journals
//! by the `"{collection}/"` prefix, matching the graph's `project_write`.

use crate::clock::Clock;
use proto_gazette::broker::LabelSet;
use std::collections::BTreeMap;

/// The single logical-partition journal name for an unpartitioned collection:
/// `{collection}/pivot=00`. It is the [`partition_journal`] of a collection with
/// no partition fields, and the name ingest / the `TestPublisher` write to and
/// the segment feeder / Verify read from when a collection is unpartitioned.
pub fn default_partition_journal(collection: &str) -> String {
    format!("{collection}/pivot=00")
}

/// One committed document and the synthetic publication clock at which it was
/// written.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredDoc {
    /// Raw JSON document bytes, as published.
    pub doc: Vec<u8>,
    /// Synthetic publication clock (monotonic across the run).
    pub clock: u64,
}

/// An append-only, per-journal document store shared across a test run.
#[derive(Default)]
pub struct CollectionStore {
    journals: BTreeMap<String, Vec<StoredDoc>>,
    /// Logical-partition label set of each journal (its `estuary.dev/field/*`
    /// labels), registered on first append. Verify matches these against a
    /// step's partition [`LabelSelector`]. Unpartitioned journals register an
    /// empty set (every field-only selector matches).
    partition_labels: BTreeMap<String, LabelSet>,
}

impl CollectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `doc` to `journal`, returning the journal's new write head (its
    /// document count).
    pub fn append(&mut self, journal: &str, doc: Vec<u8>, clock: u64) -> i64 {
        let log = self.journals.entry(journal.to_string()).or_default();
        log.push(StoredDoc { doc, clock });
        log.len() as i64
    }

    /// Record a journal's logical-partition label set (idempotent). Ingest and
    /// the `TestPublisher` call this alongside [`append`](Self::append) so Verify
    /// can filter partitions by a step's selector.
    pub fn register_partition(&mut self, journal: &str, labels: LabelSet) {
        self.partition_labels
            .entry(journal.to_string())
            .or_insert(labels);
    }

    /// The registered logical-partition label set of `journal`, or an empty set
    /// (an unpartitioned journal, or one never registered).
    pub fn partition_labels_of(&self, journal: &str) -> LabelSet {
        self.partition_labels
            .get(journal)
            .cloned()
            .unwrap_or_default()
    }

    /// The write head (document count) of `journal`, or zero if it has no docs.
    pub fn write_head(&self, journal: &str) -> i64 {
        self.journals
            .get(journal)
            .map(|l| l.len() as i64)
            .unwrap_or(0)
    }

    /// The journal names belonging to `collection` (those with a
    /// `"{collection}/"` prefix), in sorted order.
    pub fn journals_of(&self, collection: &str) -> Vec<String> {
        let prefix = format!("{collection}/");
        self.journals
            .keys()
            .filter(|j| j.starts_with(&prefix))
            .cloned()
            .collect()
    }

    /// A [`Clock`] over `collection`'s journals at their current write heads.
    /// This is the write clock the scheduler tracks after an ingest or stat.
    pub fn write_clock(&self, collection: &str) -> Clock {
        let prefix = format!("{collection}/");
        self.journals
            .iter()
            .filter(|(j, _)| j.starts_with(&prefix))
            .map(|(j, log)| (j.clone(), log.len() as i64))
            .collect()
    }

    /// Documents of `journal` in the half-open window `[from, to)` (document
    /// indices). `from` clamps to zero and `to` to the write head, so a `from`
    /// absent from a clock (zero) reads from the start and a `to` absent reads
    /// through the head — matching V1's `FetchDocuments`.
    pub fn read_window(&self, journal: &str, from: i64, to: i64) -> &[StoredDoc] {
        let Some(log) = self.journals.get(journal) else {
            return &[];
        };
        let from = from.max(0) as usize;
        let to = if to < 0 {
            log.len()
        } else {
            (to as usize).min(log.len())
        };
        if from >= to {
            return &[];
        }
        &log[from..to]
    }

    /// Read a collection's documents written in the window `(from, to]` across
    /// all its journals, in journal-sorted then append order. `from` / `to` are
    /// per-journal clocks; a journal absent from `from` reads from zero, and one
    /// absent from `to` reads through its write head. This is the Verify fetch
    /// (before partition-selector filtering, which the caller applies by passing
    /// only the matching `journals`).
    pub fn read_collection_window<'a>(
        &'a self,
        journals: &[String],
        from: &Clock,
        to: &Clock,
    ) -> Vec<&'a StoredDoc> {
        let mut out = Vec::new();
        for journal in journals {
            let from_off = from.get(journal).copied().unwrap_or(0);
            // Absent from `to` means "through the write head" (-1 sentinel).
            let to_off = to.get(journal).copied().unwrap_or(-1);
            out.extend(self.read_window(journal, from_off, to_off));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_write_head() {
        let mut store = CollectionStore::new();
        assert_eq!(store.write_head("acme/c/pivot=00"), 0);

        assert_eq!(
            store.append("acme/c/pivot=00", b"{\"k\":1}".to_vec(), 10),
            1
        );
        assert_eq!(
            store.append("acme/c/pivot=00", b"{\"k\":2}".to_vec(), 11),
            2
        );
        assert_eq!(store.write_head("acme/c/pivot=00"), 2);
    }

    #[test]
    fn journals_and_write_clock_by_prefix() {
        let mut store = CollectionStore::new();
        store.append("acme/c/pivot=00", b"a".to_vec(), 1);
        store.append("acme/c/region=eu/pivot=00", b"b".to_vec(), 2);
        store.append("acme/other/pivot=00", b"c".to_vec(), 3);

        assert_eq!(
            store.journals_of("acme/c"),
            vec![
                "acme/c/pivot=00".to_string(),
                "acme/c/region=eu/pivot=00".to_string(),
            ]
        );

        let clock = store.write_clock("acme/c");
        assert_eq!(
            clock,
            Clock::from([
                ("acme/c/pivot=00".to_string(), 1),
                ("acme/c/region=eu/pivot=00".to_string(), 1),
            ])
        );
        // The unrelated collection is excluded.
        assert!(!clock.contains_key("acme/other/pivot=00"));
    }

    #[test]
    fn read_window_is_half_open_and_clamped() {
        let mut store = CollectionStore::new();
        for i in 0..5 {
            store.append(
                "c/pivot=00",
                format!("{{\"k\":{i}}}").into_bytes(),
                i as u64,
            );
        }

        // [1, 3) yields docs 1 and 2.
        let window = store.read_window("c/pivot=00", 1, 3);
        assert_eq!(window.len(), 2);
        assert_eq!(window[0].doc, b"{\"k\":1}");
        assert_eq!(window[1].doc, b"{\"k\":2}");

        // to = -1 reads through the head.
        assert_eq!(store.read_window("c/pivot=00", 3, -1).len(), 2);
        // Clamped to head; from >= to is empty.
        assert!(store.read_window("c/pivot=00", 4, 4).is_empty());
        assert!(store.read_window("missing", 0, -1).is_empty());
    }

    #[test]
    fn read_collection_window_across_journals() {
        let mut store = CollectionStore::new();
        // Two journals of collection "c". Simulate a test-case start at head 1.
        store.append("c/pivot=00", b"old0".to_vec(), 1);
        store.append("c/pivot=01", b"old1".to_vec(), 1);
        let from = store.write_clock("c"); // {pivot=00:1, pivot=01:1}

        store.append("c/pivot=00", b"new0".to_vec(), 2);
        store.append("c/pivot=01", b"new1".to_vec(), 2);
        store.append("c/pivot=00", b"new2".to_vec(), 3);
        let to = store.write_clock("c");

        let journals = store.journals_of("c");
        let docs: Vec<&[u8]> = store
            .read_collection_window(&journals, &from, &to)
            .into_iter()
            .map(|d| d.doc.as_slice())
            .collect();

        // Only documents written after the window start, journal-sorted then
        // in append order: pivot=00 gets new0,new2; pivot=01 gets new1.
        assert_eq!(docs, vec![b"new0".as_slice(), b"new2", b"new1"]);
    }
}
