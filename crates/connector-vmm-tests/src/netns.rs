//! Sockets bound in another network namespace and handed to this unprivileged
//! process. A socket keeps the namespace it was created in, so
//! `fixtures/bind.py` creates it there as root and passes the descriptor back
//! over a Unix socket, and the test then serves on it as on any other.
//!
//! This is how the controlled endpoints live behind a routed hop instead of on
//! the host, where the host boundary rightly refuses a VMM's traffic, and how
//! a test listens inside a sibling container to see whether anything arrives.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::net::UnixListener;

pub enum Namespace<'a> {
    Named(&'a str),
    Process(u32),
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

    let pid;
    let mut argv: Vec<&str> = match namespace {
        Namespace::Named(name) => vec!["ip", "netns", "exec", name],
        Namespace::Process(process) => {
            pid = process.to_string();
            vec!["nsenter", "-t", &pid, "-n"]
        }
    };
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

fn receive(channel: &std::os::unix::net::UnixStream) -> std::io::Result<OwnedFd> {
    use std::os::fd::AsRawFd;

    let mut byte = [0u8; 2];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: byte.len(),
    };
    // Room for exactly one descriptor, aligned as a cmsghdr requires.
    let mut control = [0u64; 4];
    // SAFETY: an all-zero msghdr is valid; the pointers set below outlive
    // the recvmsg call.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = std::mem::size_of_val(&control);

    channel.set_nonblocking(false)?;
    // SAFETY: `message` describes buffers owned by this frame.
    if unsafe { libc::recvmsg(channel.as_raw_fd(), &mut message, libc::MSG_CMSG_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: recvmsg filled `control` and set its length in `message`;
    // CMSG_FIRSTHDR returns null or a header within it.
    let header = unsafe { libc::CMSG_FIRSTHDR(&message).as_ref() }
        .filter(|h| h.cmsg_level == libc::SOL_SOCKET && h.cmsg_type == libc::SCM_RIGHTS)
        .ok_or_else(|| std::io::Error::other("no descriptor in the message"))?;
    // SAFETY: an SCM_RIGHTS header's data is the descriptors it carries,
    // which this process now owns.
    let fd = unsafe { std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<libc::c_int>()) };
    // SAFETY: the descriptor was just received, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
