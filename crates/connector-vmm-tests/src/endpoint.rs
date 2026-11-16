//! A TCP endpoint, usually on one of the controlled addresses. It answers
//! every read with `pong` and never hangs up first, so a held connection's
//! survival is the ruleset's doing rather than the peer's patience. It also
//! records who connected, so a probe that timed out can be shown never to have
//! arrived. `echo` is the UDP counterpart: it answers each datagram with
//! `pong` and records its source.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::sync::{Arc, Mutex};

pub struct Endpoint {
    pub addr: SocketAddr,
    accepted: Arc<Mutex<Vec<Ipv4Addr>>>,
}

pub fn listen(listener: TcpListener) -> Endpoint {
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

pub fn echo(socket: UdpSocket) -> Endpoint {
    let addr = socket.local_addr().expect("a bound socket has an address");
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let shared = accepted.clone();

    std::thread::spawn(move || {
        let mut buffer = [0u8; 256];
        while let Ok((_, peer)) = socket.recv_from(&mut buffer) {
            if let SocketAddr::V4(peer) = peer {
                shared.lock().unwrap().push(*peer.ip());
            }
            let _ = socket.send_to(b"pong\n", peer);
        }
    });
    Endpoint { addr, accepted }
}

/// The source address of every connection accepted, or datagram received, so
/// far.
pub fn accepted(endpoint: &Endpoint) -> Vec<Ipv4Addr> {
    endpoint.accepted.lock().unwrap().clone()
}
