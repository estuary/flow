use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    io::{self, Write},
};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub struct Meta {
    uuid: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Value::is_null")]
    capture: Value,

    #[serde(default, skip_serializing_if = "Value::is_null")]
    materialize: Value,
}

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let mut uuid_conv = false;
    for uuid_str in std::env::args().skip(1) {
        let uuid = Uuid::parse_str(&uuid_str)?;
        let parts = UuidParts::from(uuid);
        serde_json::to_writer(io::stdout(), &parts)?;
        println!("");
        uuid_conv = true;
    }
    if uuid_conv {
        return Ok(());
    }

    let mut stdin = io::stdin().lock();
    let mut deser = serde_json::Deserializer::from_reader(&mut stdin).into_iter();

    let mut stats = Vec::with_capacity(4);
    while let Some(result) = deser.next() {
        let doc: Doc = result?;
        if doc.meta.ack == Some(true) {
            let issues = on_ack(doc.meta.uuid, &mut stats);
            if !issues.is_empty() {
                serde_json::to_writer(
                    &mut io::stdout(),
                    &WeirdCrap {
                        ack_offset: deser.byte_offset(),
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
    ack_offset: usize,
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

    let mut acked_docs: Vec<&'_ Doc> = Vec::with_capacity(1);

    for doc in docs.iter() {
        let doc_parts = UuidParts::from(doc.meta.uuid);
        if doc_parts.flags != FLAG_CONTINUE_TXN {
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: doc_parts.clone(),
                reason: "document flags != CONTINUE_TXN",
            })
        }

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
        } else {
            acked_docs.push(doc);
        }
    }

    if acked_docs.len() > 1 {
        // This is what we're here for!
        for doc in acked_docs {
            woah.push(Woah {
                uuid: doc.meta.uuid,
                parts: UuidParts::from(doc.meta.uuid),
                reason: "multiple stats documents in transaction",
            });
        }
    } else if acked_docs.is_empty() {
        woah.push(Woah {
            uuid: ack,
            parts: ack_parts.clone(),
            reason: "Ack of 0 documents in this batch",
        });
    }
    woah
}

const FLAG_OUTSIDE_TXN: u16 = 0x00;
const FLAG_CONTINUE_TXN: u16 = 0x01;
const FLAG_ACK_TXN: u16 = 0x02;

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct UuidParts {
    ts: String,
    clock: u64,
    producer_id: [u8; 6],
    flags: u16,
}

impl UuidParts {
    //fn is_acked_by(ack: )
}

fn timestamp(uuid_clock: u64) -> String {
    use time::format_description::well_known::Rfc3339;

    // UUID timestamps count from the gregorian calendar start, not the unix epoch.
    // This number represents the difference between the two, in units of 100 nanos.
    // We'll subract this from the uuid ts in order to translate into unix epoch time.
    const G1582NS100: i128 = 122192928000000000;

    // shift off the lowest 4 bits, which represent the sequence counter
    let ts_100nanos = uuid_clock >> 4;
    let unix_ts_100nanos = ts_100nanos as i128 - G1582NS100;

    let ts = time::OffsetDateTime::from_unix_timestamp_nanos(unix_ts_100nanos * 100)
        .expect("uuid clock could not be made into a timestamp");
    ts.format(&Rfc3339).expect("failed to format ts")
}

impl From<Uuid> for UuidParts {
    fn from(uuid: Uuid) -> Self {
        let producer_id = get_producer_id(uuid);
        let clock = get_clock(uuid);
        let flags = get_flags(uuid);
        let ts = timestamp(clock);
        UuidParts {
            ts,
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

    let mut t = (get_be_u32(&uuid_bytes[..4]) as u64) << 4;
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
    u16_bytes[..].copy_from_slice(bytes);
    u16::from_be_bytes(u16_bytes)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn uuid_is_parsed_correctly() {
        /*
        var uuidStr = "83fc2f81-cc5f-11e9-82aa-055f02a58010";
        var expectedProducer = ProducerID([6]byte{5, 95, 2, 165, 128, 16})
        require.Equal(t, expectedProducer, GetProducerID(parsed))

        var expectedClock =  Clock(0x1e9cc5f83fc2f810)
        const expectFlags = Flags(682) // 0b1010101010
            */
        let uuid_str = "83fc2f81-cc5f-11e9-82aa-055f02a58010";
        let expected = UuidParts {
            clock: 0x1e9cc5f83fc2f810u64,
            producer_id: [5, 95, 2, 165, 128, 16],
            flags: 682u16,
        };

        let parsed: Uuid = uuid_str.parse().unwrap();
        let actual = UuidParts::from(parsed);
        assert_eq!(expected, actual);
    }
}
