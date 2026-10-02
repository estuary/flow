//! A TCP endpoint on one of the controlled addresses. It answers every read
//! with `pong` and never hangs up first, so a held connection's survival is
//! the ruleset's doing rather than the peer's patience. It also records who
//! connected, so a probe that timed out can be shown never to have arrived.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};

pub struct Endpoint {
    pub addr: SocketAddr,
    accepted: Arc<Mutex<Vec<Ipv4Addr>>>,
}

pub fn listen(ip: Ipv4Addr) -> Endpoint {
    let listener =
        TcpListener::bind((ip, 0)).unwrap_or_else(|e| panic!("binding TCP on {ip}: {e}"));
    let addr = listener
        .local_addr()
        .expect("a bound listener has an address");
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let shared = accepted.clone();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            if let Ok(SocketAddr::V4(peer)) = stream.peer_addr() {
                shared.lock().unwrap().push(*peer.ip());
            }
            std::thread::spawn(move || {
                let mut buffer = [0u8; 256];
                while let Ok(read) = stream.read(&mut buffer) {
                    if read == 0 || stream.write_all(b"pong\n").is_err() {
                        return;
                    }
                }
            });
        }
    });
    Endpoint { addr, accepted }
}

/// The source address of every connection accepted so far.
pub fn accepted(endpoint: &Endpoint) -> Vec<Ipv4Addr> {
    endpoint.accepted.lock().unwrap().clone()
}
