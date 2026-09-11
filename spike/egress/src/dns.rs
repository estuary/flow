//! Just enough DNS to gate answers: walk a message, read its A records, patch
//! their TTLs in place, and synthesize the two answers the resolver invents on
//! its own (an empty AAAA answer and a REFUSED).
//!
//! Nothing here re-encodes a message. Rewriting a TTL is a four-byte store at
//! a known offset, and anything the resolver does not understand is forwarded
//! byte for byte - which is the only safe thing to do with EDNS, DNSSEC, and
//! the compression pointers a re-encoder would have to keep valid.

use std::net::Ipv4Addr;

pub const TYPE_A: u16 = 1;
pub const TYPE_AAAA: u16 = 28;
pub const RCODE_REFUSED: u16 = 5;
pub const RCODE_SERVFAIL: u16 = 2;

const HEADER_LEN: usize = 12;
const FLAG_RESPONSE: u16 = 0x8000;
const FLAG_RECURSION_AVAILABLE: u16 = 0x0080;

pub struct Header {
    pub flags: u16,
    pub qdcount: u16,
    pub ancount: u16,
}

pub struct Question {
    pub name: String,
    pub qtype: u16,
    /// Offset one past the question, where the answer section begins.
    pub end: usize,
}

pub struct ARecord {
    pub address: Ipv4Addr,
    pub ttl: u32,
    /// Offset of the record's 32-bit TTL field, for `set_ttl`.
    pub ttl_offset: usize,
}

pub fn header(message: &[u8]) -> anyhow::Result<Header> {
    if message.len() < HEADER_LEN {
        anyhow::bail!(
            "message of {} bytes is shorter than a header",
            message.len()
        );
    }
    Ok(Header {
        flags: be16(message, 2),
        qdcount: be16(message, 4),
        ancount: be16(message, 6),
    })
}

/// The first question, which is the only one the resolver ever acts on: a
/// query carrying more than one is forwarded and returned untouched.
pub fn first_question(message: &[u8]) -> anyhow::Result<Question> {
    let mut name = String::new();
    let mut cursor = read_name(message, HEADER_LEN, &mut name)?;

    if cursor + 4 > message.len() {
        anyhow::bail!("question truncated");
    }
    let qtype = be16(message, cursor);
    cursor += 4;
    Ok(Question {
        name,
        qtype,
        end: cursor,
    })
}

/// Every A record in the answer section, with where its TTL lives.
pub fn answer_a_records(message: &[u8], question: &Question) -> anyhow::Result<Vec<ARecord>> {
    let Header {
        qdcount, ancount, ..
    } = header(message)?;

    let mut cursor = question.end;
    // Questions past the first, whose names the answer section may point into.
    for _ in 1..qdcount {
        cursor = read_name(message, cursor, &mut String::new())? + 4;
    }

    let mut records = Vec::new();
    for _ in 0..ancount {
        cursor = read_name(message, cursor, &mut String::new())?;
        if cursor + 10 > message.len() {
            anyhow::bail!("resource record truncated");
        }
        let rtype = be16(message, cursor);
        let ttl_offset = cursor + 4;
        let rdlength = be16(message, cursor + 8) as usize;
        cursor += 10;

        if cursor + rdlength > message.len() {
            anyhow::bail!("resource record data truncated");
        }
        if rtype == TYPE_A && rdlength == 4 {
            records.push(ARecord {
                address: Ipv4Addr::new(
                    message[cursor],
                    message[cursor + 1],
                    message[cursor + 2],
                    message[cursor + 3],
                ),
                ttl: be32(message, ttl_offset),
                ttl_offset,
            });
        }
        cursor += rdlength;
    }
    Ok(records)
}

pub fn set_ttl(message: &mut [u8], record: &ARecord, ttl: u32) {
    message[record.ttl_offset..record.ttl_offset + 4].copy_from_slice(&ttl.to_be_bytes());
}

/// A response to `query` with no records and the given rcode, keeping the
/// question section so the client matches it to its query.
pub fn respond_empty(query: &[u8], question: &Question, rcode: u16) -> Vec<u8> {
    let mut response = query[..question.end].to_vec();
    let flags = (be16(query, 2) & 0x7900) | FLAG_RESPONSE | FLAG_RECURSION_AVAILABLE | rcode;

    response[2..4].copy_from_slice(&flags.to_be_bytes());
    response[4..6].copy_from_slice(&1u16.to_be_bytes());
    response[6..12].fill(0);
    response
}

/// Reads a name, following compression pointers, and returns the offset one
/// past the name as it appears at `start` (a pointer is two bytes, whatever it
/// points at).
fn read_name(message: &[u8], start: usize, name: &mut String) -> anyhow::Result<usize> {
    let mut cursor = start;
    let mut end = None;
    // A compression pointer may only point backwards; bounding the jumps by
    // the message length turns any cycle into an error rather than a hang.
    let mut budget = message.len();

    loop {
        let Some(&length) = message.get(cursor) else {
            anyhow::bail!("name runs past the end of the message");
        };
        if length & 0xC0 == 0xC0 {
            if cursor + 1 >= message.len() {
                anyhow::bail!("compression pointer truncated");
            }
            let target = (be16(message, cursor) & 0x3FFF) as usize;
            end.get_or_insert(cursor + 2);
            budget = budget.checked_sub(1).unwrap_or_default();
            if budget == 0 {
                anyhow::bail!("compression pointer loop");
            }
            cursor = target;
            continue;
        }
        if length & 0xC0 != 0 {
            anyhow::bail!("reserved label length {length:#x}");
        }
        cursor += 1;
        if length == 0 {
            return Ok(end.unwrap_or(cursor));
        }
        let Some(label) = message.get(cursor..cursor + length as usize) else {
            anyhow::bail!("label runs past the end of the message");
        };
        if !name.is_empty() {
            name.push('.');
        }
        name.push_str(&String::from_utf8_lossy(label));
        cursor += length as usize;
    }
}

fn be16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
}

fn be32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}
