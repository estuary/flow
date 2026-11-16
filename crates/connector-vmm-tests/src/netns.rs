//! Sockets bound in another network namespace and handed to this unprivileged
//! process. A socket keeps the namespace it was created in, so
//! `fixtures/bind.py` creates it there as root and passes the descriptor back
//! over a Unix socket, and the test then serves on it as on any other.
//!
//! This is how the controlled endpoints live behind a routed hop instead of on
//! the host, where the host boundary rightly refuses a VMM's traffic.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixListener;

pub enum Namespace<'a> {
    Named(&'a str),
}

pub fn tcp(namespace: Namespace, addr: SocketAddr) -> TcpListener {
    TcpListener::from(bind(namespace, "tcp", addr))
}

pub fn udp(namespace: Namespace, addr: SocketAddr) -> UdpSocket {
    UdpSocket::from(bind(namespace, "udp", addr))
}

fn bind(namespace: Namespace, kind: &str, addr: SocketAddr) -> OwnedFd {
    let path = std::env::temp_dir().join(format!(
        "connector-vmm-bind-{}.sock",
        crate::run::random_hex()
    ));
    let listener =
        UnixListener::bind(&path).unwrap_or_else(|e| panic!("binding {}: {e}", path.display()));

    let Namespace::Named(name) = namespace;
    let mut argv: Vec<&str> = vec!["ip", "netns", "exec", name];
    let helper = crate::run::fixture("bind.py");
    let (ip, port) = (addr.ip().to_string(), addr.port().to_string());
    argv.extend([
        "python3",
        helper.to_str().expect("a UTF-8 path"),
        path.to_str().expect("a UTF-8 path"),
        kind,
        &ip,
        &port,
    ]);
    // The helper has connected and sent by the time it exits, and the
    // listener's backlog holds that connection until it is accepted.
    let output = crate::run::sudo_output(&argv, None);
    let _ = std::fs::remove_file(&path);
    assert!(
        output.status.success(),
        "binding {kind} {addr} in another namespace: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    listener
        .set_nonblocking(true)
        .expect("a non-blocking listener");
    let (channel, _) = listener
        .accept()
        .expect("the helper connected before exiting");

    receive(&channel).unwrap_or_else(|e| panic!("receiving the {kind} {addr} descriptor: {e}"))
}

/// Exactly one descriptor. Any others the message carried are closed on return.
fn receive(channel: &std::os::unix::net::UnixStream) -> std::io::Result<OwnedFd> {
    let mut byte = [0u8; 2];
    let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
    let mut control = rustix::net::RecvAncillaryBuffer::new(&mut space);

    channel.set_nonblocking(false)?;
    let received = rustix::net::recvmsg(
        channel,
        &mut [std::io::IoSliceMut::new(&mut byte)],
        &mut control,
        rustix::net::RecvFlags::CMSG_CLOEXEC,
    )?;
    if received.flags.contains(rustix::net::ReturnFlags::CTRUNC) {
        return Err(std::io::Error::other(
            "the message carried more than its control buffer holds",
        ));
    }

    let mut fds: Vec<OwnedFd> = Vec::new();
    for message in control.drain() {
        if let rustix::net::RecvAncillaryMessage::ScmRights(rights) = message {
            fds.extend(rights);
        }
    }
    match <[OwnedFd; 1]>::try_from(fds) {
        Ok([fd]) => Ok(fd),
        Err(fds) => Err(std::io::Error::other(format!(
            "{} descriptors in the message, not 1",
            fds.len()
        ))),
    }
}
