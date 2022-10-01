use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Write};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub struct Meta {
    uuid: Uuid,
    #[serde(default)]
    ack: Option<bool>,
}

#[derive(Deserialize, Serialize)]
pub struct Doc {
    #[serde(rename = "_meta")]
    meta: Meta,
    #[serde(default)]
    shard: Value,
    #[serde(default)]
    ts: String,
    #[serde(default)]
    capture: Value,
}

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let mut stdin = io::stdin().lock();
    let mut deser = serde_json::Deserializer::from_reader(&mut stdin).into_iter();

    let mut stats = Vec::with_capacity(4);
    for result in deser {
        let doc: Doc = result?;
        if doc.meta.ack.is_some() {
            if doc.meta.ack != Some(true) {
                panic!("ack is false");
            }
            let issues = on_ack(doc.meta.uuid, &mut stats);
            if !issues.is_empty() {
                serde_json::to_writer(
                    &mut io::stdout(),
                    &WeirdCrap {
                        ack: doc.meta.uuid,
                        ack_parts: UuidParts::from(doc.meta.uuid),
                        txn_docs: stats.drain(..).collect(),
                        woah: issues,
                    },
                )?;
                io::stdout().write(b"\n")?;
            } else {
                stats.clear();
            }
        } else {
            stats.push(doc);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct WeirdCrap {
    ack: Uuid,
    ack_parts: UuidParts,
    txn_docs: Vec<Doc>,
    woah: Vec<Woah>,
}

#[derive(Serialize)]
struct Woah {
    uuid: Uuid,
    parts: UuidParts,
    reason: &'static str,
}

fn on_ack(ack: Uuid, docs: &[Doc]) -> Vec<Woah> {
    let ack_parts = UuidParts::from(ack);

    let mut woah = Vec::new();

    for (i, doc) in docs.iter().enumerate() {
        let doc_parts = UuidParts::from(doc.meta.uuid);
        if ack_parts.flags != FLAG_ACK_TXN {
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: doc_parts,
                reason: "ack flags did not ack!",
            });
        } else if doc_parts.producer_id != ack_parts.producer_id {
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: doc_parts,
                reason: "producer_id does not match",
            });
        } else if doc_parts.clock > ack_parts.clock {
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: doc_parts,
                reason: "document was rolled back",
            });
        } else if doc_parts.clock == ack_parts.clock {
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: doc_parts,
                reason: "doc clock == ack clock",
            });
        } else if i >= 1 {
            // This is what we're here for!
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: doc_parts,
                reason: "multiple stats documents in transaction",
            });
        }
    }
    woah
}

const FLAG_OUTSIDE_TXN: u16 = 0x00;
const FLAG_CONTINUE_TXN: u16 = 0x01;
const FLAG_ACK_TXN: u16 = 0x02;

#[derive(Debug, Serialize, Clone)]
pub struct UuidParts {
    clock: u64,
    producer_id: [u8; 6],
    flags: u16,
}

impl UuidParts {
    //fn is_acked_by(ack: )
}

impl From<Uuid> for UuidParts {
    fn from(uuid: Uuid) -> Self {
        let producer_id = get_producer_id(uuid);
        let clock = get_clock(uuid);
        let flags = get_flags(uuid);
        UuidParts {
            clock,
            producer_id,
            flags,
        }
    }
}

fn get_producer_id(uuid: Uuid) -> [u8; 6] {
    let (_, _, _, bytes) = uuid.as_fields();
    let mut node = [0; 6];
    node.copy_from_slice(&bytes[2..]);
    node
}

fn get_clock(uuid: Uuid) -> u64 {
    /* Gazette impl, for reference
    var t = uint64(binary.BigEndian.Uint32(uuid[0:4])) << 4 // Clock low bits.
    t |= uint64(binary.BigEndian.Uint16(uuid[4:6])) << 36   // Clock middle bits.
    t |= uint64(binary.BigEndian.Uint16(uuid[6:8])) << 52   // Clock high bits.
    t |= uint64(uuid[8]>>2) & 0xf                           // Clock sequence.
    */
    let uuid_bytes: &[u8] = uuid.as_ref();

    let mut t = get_be_u32(&uuid_bytes[..4]) as u64;
    t |= (get_be_u16(&uuid_bytes[4..6]) as u64) << 36;
    t |= (get_be_u16(&uuid_bytes[6..8]) as u64) << 52;

    t |= (uuid_bytes[8] >> 2) as u64 & 0xf;
    t
}

fn get_flags(uuid: Uuid) -> u16 {
    /* Gazette impl, for reference
    return Flags(binary.BigEndian.Uint16(uuid[8:10])) & 0x3ff
    */
    get_be_u16(&uuid.as_ref()[8..10]) & 0x3ff
}

fn get_be_u32(bytes: &[u8]) -> u32 {
    let mut u32_bytes = [0u8; 4];
    (&mut u32_bytes[..]).copy_from_slice(bytes);
    u32::from_be_bytes(u32_bytes)
}

fn get_be_u16(bytes: &[u8]) -> u16 {
    let mut u16_bytes = [0u8; 2];
    &mut u16_bytes[..].copy_from_slice(bytes);
    u16::from_be_bytes(u16_bytes)
}
