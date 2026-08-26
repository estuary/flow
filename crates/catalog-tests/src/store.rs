//! `CollectionStore`: the harness's in-memory stand-in for collection journals.
//!
//! Each logical partition of a collection is a "journal" — an append-log of
//! committed documents. Ingest and the `TestPublisher` append; Verify and the
//! shuffle segment feeder read. The store persists across test cases within a
//! run: `Reset` clears connector state, not collection data.
//!
//! Offsets are **document counts**, not byte offsets: a journal's write head is
//! the number of documents appended so far, and Verify's `(from, to]` clock
//! window maps exactly onto a half-open document-index range.
//!
//! Every document is stamped with the **transaction ordinal** current at its
//! append. The scheduler begins a transaction per ingest step and per batch of
//! concurrently-ready derivation reads, and the feeder sequences documents by
//! ordinal across journals: a document of an earlier transaction is always read
//! ahead of one of a later transaction, while order *within* a transaction is
//! deliberately unspecified — exactly the guarantee the runtime gives.
//!
//! Journal names are the real ones: [`crate::partitions`] derives each from the
//! collection's partition template, so a journal is
//! `"{collection}/{generation}/{fields...}/pivot=00"`. Both
//! [`journals_of`](CollectionStore::journals_of) and
//! [`write_clock`](CollectionStore::write_clock) therefore select a collection's
//! journals by the `"{template_name}/"` prefix.

use crate::clock::Clock;
use proto_gazette::broker::LabelSet;
use std::collections::BTreeMap;

/// The single journal name of an unpartitioned collection.
pub fn default_partition_journal(template_name: &str) -> String {
    format!("{template_name}/pivot=00")
}

/// One appended document: its raw JSON bytes as published, and the ordinal of
/// the transaction which appended it.
#[derive(Debug, PartialEq)]
pub struct StoredDoc {
    pub txn: u64,
    pub body: Vec<u8>,
}

/// A journal's append-log and the complete labels of the journal it stands in
/// for. Journals spring into existence on their first append, which also fixes
/// the labels: a journal cannot exist without them, and they never change.
#[derive(Default)]
struct Journal {
    docs: Vec<StoredDoc>,
    labels: LabelSet,
}

/// Labels of a journal that isn't in the store. A `static` rather than a
/// `const` so [`CollectionStore::partition_labels_of`] can hand out a reference.
static NO_LABELS: LabelSet = LabelSet { labels: Vec::new() };

/// An append-only, per-journal document store shared across a test run.
#[derive(Default)]
pub struct CollectionStore {
    journals: BTreeMap<String, Journal>,
    /// Ordinal stamped on appended documents; see the module docs.
    txn: u64,
}

impl CollectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin the next transaction: documents appended from now on carry a
    /// greater ordinal than every document appended before.
    pub fn begin_transaction(&mut self) {
        self.txn += 1;
    }

    /// Append `doc` to `journal` under the current transaction, returning the
    /// journal's new write head (its document count). `labels` are the
    /// journal's complete label set, which partition selectors match against;
    /// they're taken from the first append and ignored thereafter.
    pub fn append(&mut self, journal: &str, labels: LabelSet, doc: Vec<u8>) -> i64 {
        let entry = self
            .journals
            .entry(journal.to_string())
            .or_insert_with(|| Journal {
                docs: Vec::new(),
                labels,
            });
        entry.docs.push(StoredDoc {
            txn: self.txn,
            body: doc,
        });
        entry.docs.len() as i64
    }

    /// The complete label set of `journal`, or an empty set if the journal has
    /// no documents and thus doesn't exist.
    pub fn partition_labels_of(&self, journal: &str) -> &LabelSet {
        self.journals
            .get(journal)
            .map(|j| &j.labels)
            .unwrap_or(&NO_LABELS)
    }

    /// The write head (document count) of `journal`, or zero if it has no docs.
    pub fn write_head(&self, journal: &str) -> i64 {
        self.journals
            .get(journal)
            .map(|j| j.docs.len() as i64)
            .unwrap_or(0)
    }

    /// The journal names of the collection with partition template
    /// `template_name` (those with a `"{template_name}/"` prefix), in sorted
    /// order.
    pub fn journals_of(&self, template_name: &str) -> Vec<String> {
        let prefix = format!("{template_name}/");
        self.journals
            .keys()
            .filter(|j| j.starts_with(&prefix))
            .cloned()
            .collect()
    }

    /// A [`Clock`] over the journals of the collection with partition template
    /// `template_name`, at their current write heads. This is the write clock
    /// the scheduler tracks after an ingest or read.
    pub fn write_clock(&self, template_name: &str) -> Clock {
        let prefix = format!("{template_name}/");
        self.journals
            .iter()
            .filter(|(j, _)| j.starts_with(&prefix))
            .map(|(name, journal)| (name.clone(), journal.docs.len() as i64))
            .collect()
    }

    /// Documents of `journal` in the half-open window `[from, to)` (document
    /// indices). `from` clamps to zero, and a negative `to` reads through the
    /// write head.
    pub fn read_window(&self, journal: &str, from: i64, to: i64) -> &[StoredDoc] {
        let Some(journal) = self.journals.get(journal) else {
            return &[];
        };
        let docs = &journal.docs;

        let from = from.max(0) as usize;
        let to = if to < 0 {
            docs.len()
        } else {
            (to as usize).min(docs.len())
        };
        if from >= to {
            return &[];
        }
        &docs[from..to]
    }

    /// Read a collection's documents written in the window `(from, to]` across
    /// `journals`, in transaction order. `from` / `to` are per-journal clocks;
    /// a journal absent from `from` reads from zero, and one absent from `to`
    /// reads through its write head. Verify passes only the journals matching
    /// its partition selector.
    pub fn read_collection_window<'a>(
        &'a self,
        journals: &[String],
        from: &Clock,
        to: &Clock,
    ) -> Vec<&'a Vec<u8>> {
        let mut out: Vec<&StoredDoc> = Vec::new();
        for journal in journals {
            let from_off = from.get(journal).copied().unwrap_or(0);
            // Absent from `to` means "through the write head" (-1 sentinel).
            let to_off = to.get(journal).copied().unwrap_or(-1);
            out.extend(self.read_window(journal, from_off, to_off));
        }
        out.sort_by_key(|d| d.txn);
        out.into_iter().map(|d| &d.body).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A collection's partition-template name, as `partitions` uses: the
    // collection name plus its generation ID.
    const TEMPLATE: &str = "acmeCo/c/0011223344556677";
    // Template of `acmeCo/c/inner`, a collection nested *under* `acmeCo/c`.
    const NESTED_TEMPLATE: &str = "acmeCo/c/inner/8899aabbccddeeff";

    #[test]
    fn append_and_write_head() {
        let journal = default_partition_journal(TEMPLATE);
        let mut store = CollectionStore::new();
        assert_eq!(store.write_head(&journal), 0);

        assert_eq!(
            store.append(&journal, LabelSet::default(), b"{\"k\":1}".to_vec()),
            1
        );
        assert_eq!(
            store.append(&journal, LabelSet::default(), b"{\"k\":2}".to_vec()),
            2
        );
        assert_eq!(store.write_head(&journal), 2);
    }

    /// Selection is by template name, so a collection nested under another's
    /// name — legal in the catalog namespace — keeps its journals to itself.
    #[test]
    fn journals_and_write_clock_by_template_prefix() {
        let mut store = CollectionStore::new();
        store.append(
            &default_partition_journal(TEMPLATE),
            LabelSet::default(),
            b"a".to_vec(),
        );
        store.append(
            &format!("{TEMPLATE}/region=eu/pivot=00"),
            LabelSet::default(),
            b"b".to_vec(),
        );
        store.append(
            &default_partition_journal(NESTED_TEMPLATE),
            LabelSet::default(),
            b"c".to_vec(),
        );
        store.append(
            "acmeCo/other/8899/pivot=00",
            LabelSet::default(),
            b"d".to_vec(),
        );

        assert_eq!(
            store.journals_of(TEMPLATE),
            vec![
                format!("{TEMPLATE}/pivot=00"),
                format!("{TEMPLATE}/region=eu/pivot=00"),
            ]
        );
        assert_eq!(
            store.journals_of(NESTED_TEMPLATE),
            vec![format!("{NESTED_TEMPLATE}/pivot=00")]
        );

        let clock = store.write_clock(TEMPLATE);
        assert_eq!(
            clock,
            Clock::from([
                (format!("{TEMPLATE}/pivot=00"), 1),
                (format!("{TEMPLATE}/region=eu/pivot=00"), 1),
            ])
        );
        // The nested and unrelated collections are both excluded.
        assert!(!clock.contains_key(&default_partition_journal(NESTED_TEMPLATE)));
        assert!(!clock.contains_key("acmeCo/other/8899/pivot=00"));
    }

    #[test]
    fn partition_labels_are_fixed_by_the_first_append() {
        let labels = LabelSet {
            labels: vec![proto_gazette::broker::Label {
                name: "estuary.dev/field/region".to_string(),
                value: "eu".to_string(),
                prefix: false,
            }],
        };
        let mut store = CollectionStore::new();
        store.append("c/region=eu/pivot=00", labels.clone(), b"a".to_vec());
        // A later append doesn't disturb them.
        store.append("c/region=eu/pivot=00", LabelSet::default(), b"b".to_vec());

        assert_eq!(store.partition_labels_of("c/region=eu/pivot=00"), &labels);
        // A journal without documents doesn't exist, and has no labels.
        assert_eq!(
            store.partition_labels_of("c/region=us/pivot=00"),
            &NO_LABELS
        );
    }

    #[test]
    fn read_window_is_half_open_and_clamped() {
        let mut store = CollectionStore::new();
        for i in 0..5 {
            store.append(
                "c/pivot=00",
                LabelSet::default(),
                format!("{{\"k\":{i}}}").into_bytes(),
            );
        }

        // [1, 3) yields docs 1 and 2.
        let window = store.read_window("c/pivot=00", 1, 3);
        assert_eq!(window.len(), 2);
        assert_eq!(window[0].body, b"{\"k\":1}");
        assert_eq!(window[1].body, b"{\"k\":2}");

        // to = -1 reads through the head.
        assert_eq!(store.read_window("c/pivot=00", 3, -1).len(), 2);
        // Clamped to head; from >= to is empty.
        assert!(store.read_window("c/pivot=00", 4, 4).is_empty());
        assert!(store.read_window("missing", 0, -1).is_empty());
    }

    /// Appends carry the ordinal of the transaction begun most recently, across
    /// journals, so a feeder can order by transaction rather than by journal.
    #[test]
    fn appends_are_stamped_with_the_current_transaction() {
        let mut store = CollectionStore::new();
        store.begin_transaction();
        store.append("c/region=west/pivot=00", LabelSet::default(), b"w".to_vec());
        store.begin_transaction();
        store.append("c/region=east/pivot=00", LabelSet::default(), b"e".to_vec());
        store.append(
            "c/region=west/pivot=00",
            LabelSet::default(),
            b"w2".to_vec(),
        );

        let txns = |journal: &str| -> Vec<u64> {
            store
                .read_window(journal, 0, -1)
                .iter()
                .map(|d| d.txn)
                .collect()
        };
        assert_eq!(txns("c/region=west/pivot=00"), vec![1, 2]);
        assert_eq!(txns("c/region=east/pivot=00"), vec![2]);
    }

    #[test]
    fn read_collection_window_across_journals() {
        let mut store = CollectionStore::new();
        // Two journals of collection "c". Simulate a test-case start at head 1.
        store.append("c/pivot=00", LabelSet::default(), b"old0".to_vec());
        store.append("c/pivot=01", LabelSet::default(), b"old1".to_vec());
        let from = store.write_clock("c"); // {pivot=00:1, pivot=01:1}

        store.append("c/pivot=00", LabelSet::default(), b"new0".to_vec());
        store.append("c/pivot=01", LabelSet::default(), b"new1".to_vec());
        store.append("c/pivot=00", LabelSet::default(), b"new2".to_vec());
        let to = store.write_clock("c");

        let journals = store.journals_of("c");
        let docs: Vec<&[u8]> = store
            .read_collection_window(&journals, &from, &to)
            .into_iter()
            .map(|d| d.as_slice())
            .collect();

        // Only documents written after the window start. All three are of one
        // transaction, so they keep journal-sorted then append order: pivot=00
        // gets new0,new2; pivot=01 gets new1.
        assert_eq!(docs, vec![b"new0".as_slice(), b"new2", b"new1"]);

        // A later transaction's document is read after an earlier one's, even
        // when its journal sorts first.
        store.begin_transaction();
        store.append("c/pivot=01", LabelSet::default(), b"txn2-a".to_vec());
        store.begin_transaction();
        store.append("c/pivot=00", LabelSet::default(), b"txn3".to_vec());
        store.append("c/pivot=01", LabelSet::default(), b"txn2-b".to_vec());
        let to = store.write_clock("c");

        let docs: Vec<&[u8]> = store
            .read_collection_window(&journals, &from, &to)
            .into_iter()
            .map(|d| d.as_slice())
            .collect();
        assert_eq!(
            docs,
            vec![
                b"new0".as_slice(),
                b"new2",
                b"new1",
                b"txn2-a",
                b"txn3",
                b"txn2-b"
            ]
        );
    }
}
