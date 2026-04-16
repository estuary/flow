mod segment;
pub use segment::Segment;

mod reader;
pub use reader::{ReadBlock, Reader};

mod scan;
pub use scan::{BlockScan, Entry, FrontierScan, Remainder};

#[cfg(test)]
pub(crate) mod test_support {
    use crate::log::block::BlockMeta;
    use crate::log::writer::Writer;
    use crate::log::{self, reader};
    use proto_gazette::uuid;
    use std::collections::HashMap;

    /// Write a block with the given entries. Each entry is
    /// (journal_name, producer_id_byte, binding, clock_u64).
    /// All entries share the same trivial document.
    /// Returns the LSN at which the block was written.
    pub fn write_block(writer: &mut Writer, entries: &[(&str, u8, u16, u64)]) -> log::Lsn {
        let alloc = doc::HeapNode::new_allocator();
        let node = doc::HeapNode::from_serde(&serde_json::json!({"k": "v"}), &alloc).unwrap();
        let doc_bytes = bytes::Bytes::from(node.to_archive().to_vec());

        let mut journals: HashMap<String, u16> = HashMap::new();
        let mut producers: HashMap<uuid::Producer, u16> = HashMap::new();
        let mut block_entries = Vec::new();

        for &(journal, prod_id, binding, clock) in entries {
            let j_bid = {
                let next = journals.len() as u16;
                *journals.entry(journal.to_string()).or_insert(next)
            };
            let producer = crate::testing::producer(prod_id);
            let p_bid = {
                let next = producers.len() as u16;
                *producers.entry(producer).or_insert(next)
            };

            block_entries.push((
                BlockMeta {
                    binding,
                    journal_bid: j_bid,
                    producer_bid: p_bid,
                    flags: 0x8000,
                    clock,
                },
                doc_bytes.len() as u32,
                bytes::Bytes::from_static(b"packed_key_prefix"),
                doc_bytes.clone(),
            ));
        }

        let (lsn, mut sealed) = writer
            .append_block(journals, producers, block_entries)
            .unwrap();

        // Disarm so the sealed segment file persists for reader tests.
        if let Some(ref mut s) = sealed {
            s.disarm();
        }

        lsn
    }

    /// Build a Frontier with the given journals and per-member flushed LSNs.
    pub fn make_frontier(
        flushed_lsns: &[log::Lsn],
        journals: Vec<crate::JournalFrontier>,
    ) -> crate::Frontier {
        let raw: Vec<u64> = flushed_lsns.iter().map(|l| l.as_u64()).collect();
        crate::Frontier::new(journals, crate::testing::terminal_chunk(raw)).unwrap()
    }

    /// Build a ProducerFrontier with a raw Clock value (not seconds).
    pub fn pf_raw(id: u8, last_commit: u64) -> crate::ProducerFrontier {
        crate::ProducerFrontier {
            producer: crate::testing::producer(id),
            last_commit: uuid::Clock::from_u64(last_commit),
            hinted_commit: uuid::Clock::from_u64(0),
            offset: 0,
        }
    }

    /// Drive a FrontierScan to completion, collecting
    /// (journal_name, producer_id_byte, binding, clock) for each entry.
    pub fn collect_entries(
        mut scan: reader::FrontierScan,
    ) -> (Vec<(String, u8, u16, u64)>, reader::FrontierScan) {
        let mut out = Vec::new();
        while scan.advance_block().unwrap() {
            for entry in scan.block_iter() {
                let producer_byte = entry.producer.producer[0];
                out.push((
                    entry.journal.name.as_str().to_owned(),
                    producer_byte,
                    entry.meta.binding.to_native(),
                    entry.meta.clock.to_native(),
                ));
            }
        }
        (out, scan)
    }
}
