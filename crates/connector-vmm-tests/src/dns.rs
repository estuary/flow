//! The scripted upstream nameserver a test's VMM forwards to through
//! `--resolver-upstream`. It answers only what the test scripted and says
//! NXDOMAIN to everything else: it never forwards, so no deterministic test
//! depends on, or sends a query to, a real nameserver.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub enum Record {
    A(Ipv4Addr, u32),
    /// A target and this record's own TTL. The target's records follow it in
    /// the same answer, as a recursive resolver returns a chain.
    Cname(String, u32),
}

pub struct Nameserver {
    pub addr: SocketAddr,
    zone: Arc<Mutex<Zone>>,
}

#[derive(Default)]
struct Zone {
    records: HashMap<String, Vec<Record>>,
    queries: Vec<String>,
}

/// Serve on an ephemeral port of `ip`, one of the controlled endpoints.
pub fn serve(ip: Ipv4Addr) -> Nameserver {
    let socket = UdpSocket::bind((ip, 0)).unwrap_or_else(|e| panic!("binding DNS on {ip}: {e}"));
    let addr = socket.local_addr().expect("a bound socket has an address");
    let zone = Arc::new(Mutex::new(Zone::default()));
    let shared = zone.clone();

    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        loop {
            let Ok((read, peer)) = socket.recv_from(&mut buffer) else {
                return;
            };
            let Some((name, reply)) = answer(&buffer[..read], &shared.lock().unwrap().records)
            else {
                continue;
            };
            shared.lock().unwrap().queries.push(name);
            let _ = socket.send_to(&reply, peer);
        }
    });
    Nameserver { addr, zone }
}

/// Replace what `name` answers. Names are lowercase.
pub fn set(nameserver: &Nameserver, name: &str, records: Vec<Record>) {
    nameserver
        .zone
        .lock()
        .unwrap()
        .records
        .insert(name.to_string(), records);
}

pub fn queries(nameserver: &Nameserver) -> Vec<String> {
    nameserver.zone.lock().unwrap().queries.clone()
}

/// The response to one query, and the name it asked for. `None` for anything
/// that is not a well-formed single-question query.
fn answer(query: &[u8], records: &HashMap<String, Vec<Record>>) -> Option<(String, Vec<u8>)> {
    if query.len() < 12 || u16::from_be_bytes([query[4], query[5]]) != 1 {
        return None;
    }
    let (name, end) = read_name(query, 12)?;
    let question = query.get(12..end + 4)?;
    let qtype = u16::from_be_bytes([query[end], query[end + 1]]);

    let mut answers: Vec<Vec<u8>> = Vec::new();
    let mut owner = name.clone();
    // Bounded, so a scripted CNAME loop cannot hang the server.
    for _ in 0..8 {
        let Some(chain) = records.get(&owner) else {
            break;
        };
        let mut next = None;
        for record in chain {
            match record {
                Record::A(addr, ttl) if qtype == 1 => {
                    answers.push(resource(&owner, 1, *ttl, &addr.octets()));
                }
                Record::A(..) => {}
                Record::Cname(target, ttl) => {
                    answers.push(resource(&owner, 5, *ttl, &encode_name(target)));
                    next = Some(target.clone());
                }
            }
        }
        let Some(target) = next else { break };
        owner = target;
    }

    let rcode: u16 = if records.contains_key(&name) { 0 } else { 3 };
    let mut reply = Vec::new();
    reply.extend_from_slice(&query[0..2]);
    // QR, RD copied, RA.
    let flags = 0x8080 | (u16::from_be_bytes([query[2], query[3]]) & 0x0100) | rcode;
    reply.extend_from_slice(&flags.to_be_bytes());
    reply.extend_from_slice(&1u16.to_be_bytes());
    reply.extend_from_slice(&(answers.len() as u16).to_be_bytes());
    reply.extend_from_slice(&[0, 0, 0, 0]);
    reply.extend_from_slice(question);
    for answer in answers {
        reply.extend_from_slice(&answer);
    }
    Some((name, reply))
}

fn resource(owner: &str, rtype: u16, ttl: u32, data: &[u8]) -> Vec<u8> {
    let mut out = encode_name(owner);
    out.extend_from_slice(&rtype.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&ttl.to_be_bytes());
    out.extend_from_slice(&(data.len() as u16).to_be_bytes());
    out.extend_from_slice(data);
    out
}

fn encode_name(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for label in name.split('.').filter(|label| !label.is_empty()) {
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out
}

/// An uncompressed question name, lowercased, and the offset just past it.
fn read_name(message: &[u8], mut cursor: usize) -> Option<(String, usize)> {
    let mut labels = Vec::new();
    loop {
        let length = *message.get(cursor)? as usize;
        cursor += 1;
        if length == 0 {
            return Some((labels.join("."), cursor));
        }
        if length > 63 {
            return None;
        }
        let label = message.get(cursor..cursor + length)?;
        labels.push(String::from_utf8_lossy(label).to_ascii_lowercase());
        cursor += length;
    }
}
