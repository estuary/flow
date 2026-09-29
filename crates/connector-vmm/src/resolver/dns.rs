//! Just enough DNS to gate answers: walk a message, read the A and CNAME
//! records of its answer section, patch their TTLs in place, and synthesize
//! the replies the resolver invents on its own.
//!
//! Nothing here re-encodes a message. Rewriting a TTL is a four-byte store at
//! a known offset, and anything not understood is forwarded byte for byte,
//! which is the only safe thing to do with EDNS, DNSSEC and the compression
//! pointers a re-encoder would have to keep valid.
//!
//! Every name this module produces is ASCII-lowercased as it is read, so
//! callers compare names without repeating that themselves. The bytes on the
//! wire keep whatever case they arrived with. A dot or backslash inside a label
//! is written as its RFC 1035 decimal escape, so the dots of a produced name
//! are exactly its label boundaries on the wire and callers may compare names
//! as strings.

use std::net::Ipv4Addr;

pub const TYPE_A: u16 = 1;
pub const TYPE_CNAME: u16 = 5;
pub const TYPE_AAAA: u16 = 28;
pub const CLASS_IN: u16 = 1;

pub const RCODE_NOERROR: u16 = 0;
pub const RCODE_SERVFAIL: u16 = 2;
pub const RCODE_REFUSED: u16 = 5;

const HEADER_LEN: usize = 12;
pub const FLAG_RESPONSE: u16 = 0x8000;
const FLAG_RECURSION_AVAILABLE: u16 = 0x0080;
/// Opcode and the recursion-desired bit, which a reply echoes back.
const FLAGS_ECHOED: u16 = 0x7900;

/// A name is at most 255 bytes on the wire, and a compression chain must not
/// be allowed to assemble anything longer than that.
const NAME_MAX: usize = 255;

pub struct Header {
    pub id: u16,
    pub flags: u16,
    pub qdcount: u16,
    pub ancount: u16,
}

pub struct Question {
    pub name: String,
    pub qtype: u16,
    pub qclass: u16,
    /// Offset one past the question, where the answer section begins and where
    /// a synthesized reply is truncated.
    pub end: usize,
}

pub struct Record {
    pub kind: Kind,
    pub ttl: u32,
    /// Offset of the record's 32-bit TTL field, for `set_ttl`.
    pub ttl_offset: usize,
}

pub enum Kind {
    Address(Ipv4Addr),
    Target(String),
}

pub fn header(message: &[u8]) -> anyhow::Result<Header> {
    if message.len() < HEADER_LEN {
        anyhow::bail!(
            "message of {} bytes is shorter than a header",
            message.len()
        );
    }
    Ok(Header {
        id: be16(message, 0),
        flags: be16(message, 2),
        qdcount: be16(message, 4),
        ancount: be16(message, 6),
    })
}

/// The first question, which is the only one the resolver ever acts on: a
/// query carrying more than one is forwarded and returned untouched.
pub fn first_question(message: &[u8]) -> anyhow::Result<Question> {
    let mut name = String::new();
    let cursor = read_name(message, HEADER_LEN, &mut name)?;

    if cursor + 4 > message.len() {
        anyhow::bail!("question truncated");
    }
    Ok(Question {
        name,
        qtype: be16(message, cursor),
        qclass: be16(message, cursor + 2),
        end: cursor + 4,
    })
}

/// Every class-IN A and CNAME record of the answer section, with where each
/// one's TTL lives. The authority and additional sections are never read: the
/// guest resolves from the answer section, so nothing else can authorize a
/// destination.
pub fn answer_records(message: &[u8]) -> anyhow::Result<Vec<Record>> {
    let header = header(message)?;
    let mut cursor = HEADER_LEN;

    for _ in 0..header.qdcount {
        cursor = read_name(message, cursor, &mut String::new())? + 4;
        if cursor > message.len() {
            anyhow::bail!("question truncated");
        }
    }

    let mut records = Vec::new();
    for _ in 0..header.ancount {
        cursor = read_name(message, cursor, &mut String::new())?;
        if cursor + 10 > message.len() {
            anyhow::bail!("resource record truncated");
        }
        let rtype = be16(message, cursor);
        let rclass = be16(message, cursor + 2);
        let ttl_offset = cursor + 4;
        let rdlength = be16(message, cursor + 8) as usize;
        cursor += 10;

        if cursor + rdlength > message.len() {
            anyhow::bail!("resource record data truncated");
        }
        if rclass == CLASS_IN {
            let ttl = be32(message, ttl_offset);

            if rtype == TYPE_A && rdlength == 4 {
                let address = Ipv4Addr::new(
                    message[cursor],
                    message[cursor + 1],
                    message[cursor + 2],
                    message[cursor + 3],
                );
                records.push(Record {
                    kind: Kind::Address(address),
                    ttl,
                    ttl_offset,
                });
            } else if rtype == TYPE_CNAME {
                let mut target = String::new();
                read_name(message, cursor, &mut target)?;

                // The root is not a name anything can be re-queried under.
                if !target.is_empty() {
                    records.push(Record {
                        kind: Kind::Target(target),
                        ttl,
                        ttl_offset,
                    });
                }
            }
        }
        cursor += rdlength;
    }
    Ok(records)
}

pub fn set_ttl(message: &mut [u8], record: &Record, ttl: u32) {
    message[record.ttl_offset..record.ttl_offset + 4].copy_from_slice(&ttl.to_be_bytes());
}

/// A reply to `query` with no records and the given rcode, keeping the
/// question section so the client matches it to its query. Any EDNS option the
/// query carried is dropped with the rest of the additional section, which a
/// requestor must already handle as "no EDNS here".
pub fn respond(query: &[u8], question: &Question, rcode: u16) -> Vec<u8> {
    let mut response = query[..question.end].to_vec();
    let flags = (be16(query, 2) & FLAGS_ECHOED) | FLAG_RESPONSE | FLAG_RECURSION_AVAILABLE | rcode;

    response[2..4].copy_from_slice(&flags.to_be_bytes());
    response[4..6].copy_from_slice(&1u16.to_be_bytes());
    response[6..12].fill(0);
    response
}

/// Reads a name, lowercasing and escaping it and following compression
/// pointers, and returns the offset one past the name as it appears at `start`
/// (a pointer is two bytes, whatever it points at).
fn read_name(message: &[u8], start: usize, name: &mut String) -> anyhow::Result<usize> {
    let mut cursor = start;
    let mut end = None;

    loop {
        let Some(&length) = message.get(cursor) else {
            anyhow::bail!("name runs past the end of the message");
        };
        if length & 0xC0 == 0xC0 {
            if cursor + 1 >= message.len() {
                anyhow::bail!("compression pointer truncated");
            }
            // RFC 1035 pointers name a prior occurrence. Requiring that makes
            // each jump strictly decrease the offset, so a cycle cannot form
            // and the walk needs no separate budget.
            let target = (be16(message, cursor) & 0x3FFF) as usize;
            if target >= cursor {
                anyhow::bail!("compression pointer at {cursor} does not point backwards");
            }
            end.get_or_insert(cursor + 2);
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
        if name.len() + 1 + label.len() > NAME_MAX {
            anyhow::bail!("name is longer than {NAME_MAX} bytes");
        }
        if !name.is_empty() {
            name.push('.');
        }
        for byte in label {
            // Unescaped, a label `eu.mirror` beneath `acmeco.example` would
            // read as a name beneath `mirror.acmeco.example`, and the gate
            // would admit it by an entry for a zone the query never reaches.
            match byte {
                b'.' => name.push_str("\\046"),
                b'\\' => name.push_str("\\092"),
                _ => name.push(byte.to_ascii_lowercase() as char),
            }
        }
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    fn encode_name(name: &str, out: &mut Vec<u8>) {
        for label in name.split('.') {
            out.push(label.len() as u8);
            out.extend(label.as_bytes());
        }
        out.push(0);
    }

    fn message(question: &str, qtype: u16, ancount: u16, answer: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(0x1234u16.to_be_bytes());
        out.extend(0x8180u16.to_be_bytes());
        out.extend(1u16.to_be_bytes());
        out.extend(ancount.to_be_bytes());
        out.extend([0u8; 4]);
        encode_name(question, &mut out);
        out.extend(qtype.to_be_bytes());
        out.extend(super::CLASS_IN.to_be_bytes());
        out.extend(answer);
        out
    }

    fn a_record(owner: &str, address: Ipv4Addr, ttl: u32, class: u16) -> Vec<u8> {
        let mut out = Vec::new();
        encode_name(owner, &mut out);
        out.extend(super::TYPE_A.to_be_bytes());
        out.extend(class.to_be_bytes());
        out.extend(ttl.to_be_bytes());
        out.extend(4u16.to_be_bytes());
        out.extend(address.octets());
        out
    }

    #[test]
    fn parsing() {
        let mut table = String::new();

        for (label, message) in cases() {
            table.push_str(&format!("# {label}\n"));

            match super::answer_records(&message) {
                Ok(records) => {
                    let rendered: Vec<String> = records
                        .iter()
                        .map(|record| match &record.kind {
                            super::Kind::Address(address) => {
                                format!("A {address} ttl {}", record.ttl)
                            }
                            super::Kind::Target(target) => {
                                format!("CNAME {target} ttl {}", record.ttl)
                            }
                        })
                        .collect();
                    table.push_str(&format!("records: [{}]\n\n", rendered.join("; ")));
                }
                Err(error) => table.push_str(&format!("refused: {error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }

    fn cases() -> Vec<(&'static str, Vec<u8>)> {
        let address = Ipv4Addr::new(93, 184, 216, 34);

        // A compression pointer back to the question's name, which is what a
        // real nameserver sends.
        let mut backward = Vec::new();
        backward.extend(0xC00Cu16.to_be_bytes());
        backward.extend(super::TYPE_A.to_be_bytes());
        backward.extend(super::CLASS_IN.to_be_bytes());
        backward.extend(300u32.to_be_bytes());
        backward.extend(4u16.to_be_bytes());
        backward.extend(address.octets());

        // A pointer to a later offset, which no legitimate message contains
        // and which is how a cycle would be built.
        let mut forward = backward.clone();
        forward[0..2].copy_from_slice(&0xC0FFu16.to_be_bytes());

        let mut root_target = Vec::new();
        encode_name("acmeco.example", &mut root_target);
        root_target.extend(super::TYPE_CNAME.to_be_bytes());
        root_target.extend(super::CLASS_IN.to_be_bytes());
        root_target.extend(300u32.to_be_bytes());
        root_target.extend(1u16.to_be_bytes());
        root_target.push(0);

        let mut long_label = Vec::new();
        long_label.push(64); // above the 63-byte maximum, but not a pointer
        long_label.extend([b'a'; 64]);
        long_label.push(0);
        long_label.extend(super::TYPE_A.to_be_bytes());
        long_label.extend(super::CLASS_IN.to_be_bytes());
        long_label.extend(300u32.to_be_bytes());
        long_label.extend(4u16.to_be_bytes());
        long_label.extend(address.octets());

        let mut reserved = long_label.clone();
        reserved[0] = 0x80; // a reserved label length

        let mut overlong_rdata = a_record("acmeco.example", address, 300, super::CLASS_IN);
        let rdlength = overlong_rdata.len() - 4 - 2;
        overlong_rdata[rdlength..rdlength + 2].copy_from_slice(&4096u16.to_be_bytes());

        vec![
            ("shorter than a header", vec![0u8; 8]),
            (
                "one A record",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                ),
            ),
            (
                "an A record in another class",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, 3),
                ),
            ),
            (
                "the owner name as a backward compression pointer",
                message("acmeco.example", super::TYPE_A, 1, &backward),
            ),
            (
                "a forward compression pointer",
                message("acmeco.example", super::TYPE_A, 1, &forward),
            ),
            (
                "a CNAME target of the root",
                message("acmeco.example", super::TYPE_A, 1, &root_target),
            ),
            (
                "a label above 63 bytes",
                message("acmeco.example", super::TYPE_A, 1, &long_label),
            ),
            (
                "a reserved label length",
                message("acmeco.example", super::TYPE_A, 1, &reserved),
            ),
            (
                "rdlength running past the message",
                message("acmeco.example", super::TYPE_A, 1, &overlong_rdata),
            ),
            (
                "more answers claimed than carried",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    4,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                ),
            ),
            ("a record past the answer section is not read", {
                let mut message = message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                );
                // One answer, and an additional record the walk must stop
                // short of: only the answer section authorizes anything.
                message[10..12].copy_from_slice(&1u16.to_be_bytes());
                message.extend(a_record(
                    "glue.acmeco.example",
                    Ipv4Addr::new(10, 0, 0, 1),
                    300,
                    super::CLASS_IN,
                ));
                message
            }),
            ("a truncated question", {
                let mut message = message("acmeco.example", super::TYPE_A, 0, &[]);
                message.truncate(message.len() - 3);
                message
            }),
            ("a truncated resource record", {
                let mut message = message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                );
                message.truncate(message.len() - 6);
                message
            }),
        ]
    }

    #[test]
    fn a_question_is_lowercased() {
        let message = message("PyPI.ORG", super::TYPE_A, 0, &[]);
        let question = super::first_question(&message).expect("the question parses");

        assert_eq!(question.name, "pypi.org");
        assert_eq!(question.qtype, super::TYPE_A);
        assert_eq!(question.qclass, super::CLASS_IN);
    }

    #[test]
    fn a_dot_inside_a_label_is_escaped() {
        let mut message = message("eu-mirror.a-b.example", super::TYPE_A, 0, &[]);
        message[12 + 1 + 2] = b'.';
        message[12 + 1 + 9 + 1 + 1] = b'\\';
        let question = super::first_question(&message).expect("the question parses");

        assert_eq!(question.name, r"eu\046mirror.a\092b.example");
    }

    #[test]
    fn a_synthesized_reply_keeps_the_question() {
        let query = {
            let mut query = message("acmeco.example", super::TYPE_AAAA, 0, &[]);
            query[2..4].copy_from_slice(&0x0100u16.to_be_bytes()); // a query, RD set
            // An EDNS OPT record in the additional section, which the reply
            // does not carry back.
            query[10..12].copy_from_slice(&1u16.to_be_bytes());
            query.extend([0, 0, 41, 16, 0, 0, 0, 0, 0, 0, 0]);
            query
        };
        let question = super::first_question(&query).expect("the question parses");
        let reply = super::respond(&query, &question, super::RCODE_REFUSED);

        let header = super::header(&reply).expect("the reply parses");
        assert_eq!(header.id, 0x1234);
        assert_eq!(header.flags & 0x000F, super::RCODE_REFUSED);
        assert_ne!(header.flags & super::FLAG_RESPONSE, 0);
        assert_eq!(header.qdcount, 1);
        assert_eq!(header.ancount, 0);
        assert_eq!(reply.len(), question.end, "every section past the question");

        let echoed = super::first_question(&reply).expect("the question survives");
        assert_eq!(echoed.name, "acmeco.example");
        assert_eq!(echoed.qtype, super::TYPE_AAAA);
    }
}
