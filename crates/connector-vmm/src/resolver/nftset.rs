//! The resolver's writes to the `resolved` set, as nfnetlink batches of
//! `netlink_packet_netfilter` messages.
//!
//! One batch shape: `NFNL_MSG_BATCH_BEGIN`, one `NFT_MSG_DELSETELEM` per
//! address being refreshed, one `NFT_MSG_NEWSETELEM` carrying every address
//! with its timeout, `NFNL_MSG_BATCH_END`. nf_tables applies a batch as one
//! transaction, so an address that is deleted and re-added in the same batch
//! is never absent from the set and a connection through it is never
//! interrupted.
//!
//! Sending the fixed batch shape directly avoids spawning `nft` per answer.
//!
//! nfnetlink handles a batch synchronously within its `send`; all replies
//! are queued before it returns. Only the insert requests a success ACK.
//! Errors are sent regardless of ACK flags, including commit/header errors
//! on the opening marker. An inner success can survive an aborted batch.
//!
//! A separate `send` requests an ACK for `NLMSG_NOOP` (the fence). Commit
//! requires the insert's ACK, then the fence's, with no batch error. This
//! also handles Linux 6.8, which does not ACK successful markers. The fence
//! needs its own send because nfnetlink stops reading at the batch's end.
//!
//! Receive overflow drops later replies, including the fence's, until the
//! queue empties and receive fails with `ENOBUFS`. A lost batch error cannot
//! be hidden by a successful fence.
//!
//! Linux 6.8 ordering:
//! - [Synchronous kernel receive](https://github.com/torvalds/linux/blob/v6.8/net/netlink/af_netlink.c#L1329-L1341).
//! - [Control-message acknowledgment](https://github.com/torvalds/linux/blob/v6.8/net/netlink/af_netlink.c#L2539-L2549).
//! - [Batch end](https://github.com/torvalds/linux/blob/v6.8/net/netfilter/nfnetlink.c#L464-L466) and
//!   [commit errors followed by queued replies](https://github.com/torvalds/linux/blob/v6.8/net/netfilter/nfnetlink.c#L561-L595).
//!
//! What the kernel was measured to do, on 7.0.0-1011-gcp with nftables 1.0.9:
//!
//! - Deleting an element that is not present fails with `ENOENT` and takes the
//!   paired insert down with it, which `apply` recovers from below.
//! - Re-adding a live element without `NLM_F_EXCL` silently replaces its
//!   timeout, including shortening it. With `NLM_F_EXCL` it fails with
//!   `EEXIST`, which is why the insert carries that flag: a surviving element
//!   means this module's model of the set is wrong, and that is worth hearing
//!   about rather than absorbing.
//! - Expired keys avoid `EEXIST` collisions. They still count against `size`
//!   until GC removes them, so inserts can fail with `ENFILE`:
//!   <https://github.com/torvalds/linux/blob/v6.8/net/netfilter/nf_tables_api.c#L7035-L7039>.
//! - Each reply is its own datagram. On 7.0.0-1014-gcp, 259 of them overflowed
//!   the default 212992-byte receive buffer, and the receive failed with
//!   `ENOBUFS`. A committed batch has two replies, the insert's and the
//!   fence's, and a failed one has a reply per failed message, so the buffer
//!   is sized for a whole set's worth (`RECEIVE_BUFFER`).
//! - A failed message's reply carries the whole message: a failed insert at
//!   the set's capacity is answered in one datagram of 28748 bytes.

use netlink_packet_core::Parseable as _;
use netlink_packet_netfilter::nftables;
use std::net::Ipv4Addr;
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

/// The table and set the ruleset renders. `tests::names_match_the_ruleset`
/// holds these against the rendered text.
const TABLE: &str = "flow_egress";
const SET: &str = "resolved";

/// The deadline for one call to `apply`, covering every receive and every
/// resend. A batch that has not committed by then leaves the set in a state
/// this module cannot describe, which is fatal, so the bound has to be on the
/// whole call rather than on each receive.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Failed inserts echo the whole message; `tests::batch_encoding_at_capacity`
/// bounds its maximum length.
const REPLY_DATAGRAM: usize = 64 * 1024;

/// Per-reply queue allowance; `tests::real_kernel_at_capacity` checks `truesize`.
const REPLY_BYTES: usize = 2048;

/// All batch replies queue before the first read. Budget one per failed
/// delete, plus the insert, batch failure and fence, with room for the
/// largest insert echo.
const RECEIVE_BUFFER: usize = (crate::ruleset::RESOLVED_SIZE + 3) * REPLY_BYTES + REPLY_DATAGRAM;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Element {
    pub address: Ipv4Addr,
    pub timeout: Duration,
}

/// Behind a trait so the resolver's tests can record what it asked for and
/// inject failures without a netlink socket.
pub trait SetWriter: Send {
    /// Delete every address in `deletes`, then insert every element of `adds`,
    /// as one transaction. Returning `Ok` means the kernel committed it.
    /// `adds` is never empty: an answer with no address to authorize does not
    /// reach the set at all.
    fn apply(&mut self, deletes: &[Ipv4Addr], adds: &[Element]) -> anyhow::Result<()>;
}

pub struct Netlink {
    socket: OwnedFd,
    sequence: u32,
}

/// Why a transaction did not commit. `Absent` is the one outcome `apply`
/// recovers from rather than reports.
enum Failure {
    /// The deletes at these indices named elements the kernel no longer holds.
    Absent(std::collections::BTreeSet<usize>),
    Other(anyhow::Error),
}

impl Netlink {
    pub fn open() -> anyhow::Result<Self> {
        let netlink = Self::connect()?;

        // Forced, which CAP_NET_ADMIN allows, because the host's
        // net.core.rmem_max caps a buffer set the ordinary way.
        rustix::net::sockopt::set_socket_recv_buffer_size_force(&netlink.socket, RECEIVE_BUFFER)
            .map_err(|e| anyhow::anyhow!(e).context("sizing the netlink receive buffer"))?;
        let effective = rustix::net::sockopt::socket_recv_buffer_size(&netlink.socket)
            .map_err(|e| anyhow::anyhow!(e).context("reading the netlink receive buffer size"))?;
        if effective < RECEIVE_BUFFER {
            anyhow::bail!(
                "the netlink receive buffer is {effective} bytes, below the {RECEIVE_BUFFER} a batch's replies can take"
            );
        }
        Ok(netlink)
    }

    fn connect() -> anyhow::Result<Self> {
        let socket = rustix::net::socket_with(
            rustix::net::AddressFamily::NETLINK,
            rustix::net::SocketType::RAW,
            rustix::net::SocketFlags::CLOEXEC,
            Some(rustix::net::netlink::NETFILTER),
        )
        .map_err(|e| anyhow::anyhow!(e).context("opening a netlink socket"))?;

        // Port 0 and no groups: the kernel, and it assigns our port.
        rustix::net::connect(&socket, &rustix::net::netlink::SocketAddrNetlink::new(0, 0))
            .map_err(|e| {
                anyhow::anyhow!(e).context("connecting the netlink socket to the kernel")
            })?;

        Ok(Netlink {
            socket,
            sequence: 1,
        })
    }

    /// Each receive waits only for what is left of the batch's deadline.
    fn set_receive_timeout(&self, remaining: Duration) -> rustix::io::Result<()> {
        // Socket timeouts have microsecond resolution.
        let remaining = remaining.max(Duration::from_micros(1));
        rustix::net::sockopt::set_socket_timeout(
            &self.socket,
            rustix::net::sockopt::Timeout::Recv,
            Some(remaining),
        )
    }

    fn transaction(
        &mut self,
        deletes: &[Ipv4Addr],
        adds: &[Element],
        deadline: Instant,
    ) -> Result<(), Failure> {
        let first = self.sequence;
        let batch = encode_batch(TABLE, SET, deletes, adds, first);
        let last = first + deletes.len() as u32 + 2;
        let fence = encode_fence(last + 1);
        // Advance whether or not this transaction succeeds, so a reply to an
        // abandoned batch can never be read as a reply to the next one.
        self.sequence = last + 2;

        // The fence needs a `send` of its own: nfnetlink reads nothing of a
        // datagram past its batch's closing marker.
        for (datagram, what) in [(&batch, "batch"), (&fence, "fence")] {
            if let Err(error) =
                rustix::net::send(&self.socket, datagram, rustix::net::SendFlags::empty())
            {
                return Err(Failure::Other(
                    anyhow::anyhow!(error).context(format!("sending a netlink {what}")),
                ));
            }
        }
        self.read_acknowledgments(Acknowledgments::new(first, last, deletes.len()), deadline)
    }

    fn read_acknowledgments(
        &self,
        mut acknowledgments: Acknowledgments,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let mut buffer = vec![0u8; REPLY_DATAGRAM];

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Failure::Other(anyhow::anyhow!(
                    "a netlink batch was not committed within {TIMEOUT:?}"
                )));
            }
            if let Err(error) = self.set_receive_timeout(remaining) {
                return Err(Failure::Other(
                    anyhow::anyhow!(error).context("setting the netlink receive timeout"),
                ));
            }
            // TRUNC makes recv report a datagram's full length, so a reply
            // larger than the buffer is refused rather than parsed in part.
            let read =
                match rustix::net::recv(&self.socket, &mut buffer, rustix::net::RecvFlags::TRUNC) {
                    Ok((_, 0)) => {
                        return Err(Failure::Other(anyhow::anyhow!(
                            "no acknowledgment for the netlink batch: an empty datagram"
                        )));
                    }
                    Ok((_, read)) if read > buffer.len() => {
                        return Err(Failure::Other(anyhow::anyhow!(
                            "netlink reply of {read} bytes overflows the {}-byte buffer",
                            buffer.len()
                        )));
                    }
                    Ok((_, read)) => read,
                    Err(error) => {
                        return Err(Failure::Other(anyhow::anyhow!(
                            "no acknowledgment for the netlink batch: {error}"
                        )));
                    }
                };
            if acknowledgments.receive(&buffer[..read])? {
                return Ok(());
            }
        }
    }
}

/// Tracks the replies to a batch running from `first` through its closing
/// marker `last`, whose insert is `last - 1`, and to its fence, `last + 1`.
struct Acknowledgments {
    first: u32,
    last: u32,
    deletes: usize,
    inserted: bool,
    absent: std::collections::BTreeSet<usize>,
}

impl Acknowledgments {
    fn new(first: u32, last: u32, deletes: usize) -> Self {
        Acknowledgments {
            first,
            last,
            deletes,
            inserted: false,
            absent: Default::default(),
        }
    }

    /// Read the whole datagram before reporting a commit. The fence must
    /// follow the insert's ACK; trailing errors or malformed messages fail.
    /// Deletes of absent elements are reported together, once the fence is
    /// read; any other failure is reported at once.
    fn receive(&mut self, datagram: &[u8]) -> Result<bool, Failure> {
        let fence = self.last + 1;
        let mut fenced = false;
        let mut offset = 0;

        while offset < datagram.len() {
            let message = netlink_packet_core::NetlinkBuffer::new_checked(&datagram[offset..])
                .map_err(|error| {
                    Failure::Other(
                        anyhow::anyhow!(error)
                            .context(format!("netlink reply at offset {offset} is malformed")),
                    )
                })?;
            let length = message.length() as usize;
            // The kernel pads every message to a 4-byte boundary, the last
            // one included.
            let next = offset + length.next_multiple_of(4);
            if next > datagram.len() {
                return Err(Failure::Other(anyhow::anyhow!(
                    "netlink reply of {length} bytes at offset {offset} overruns its {}-byte datagram once padded",
                    datagram.len()
                )));
            }
            offset = next;

            // Replies to an abandoned batch carry an older sequence. Nothing
            // but an acknowledgment of this batch is decoded further.
            let sequence = message.sequence_number();
            if message.message_type() != netlink_packet_core::NLMSG_ERROR
                || !(self.first..=fence).contains(&sequence)
            {
                continue;
            }
            let payload = message.payload();
            let acknowledgment = netlink_packet_core::ErrorBuffer::new_checked(&payload)
                .and_then(|buffer| netlink_packet_core::ErrorMessage::parse(&buffer))
                .map_err(|error| {
                    Failure::Other(anyhow::anyhow!(error).context(format!(
                        "acknowledgment of netlink message {sequence} is malformed"
                    )))
                })?;

            if let Some(code) = acknowledgment.code {
                // Reject malformed error codes that cannot be a negated errno.
                let Some(errno) = code.get().checked_neg().filter(|errno| *errno > 0) else {
                    return Err(Failure::Other(anyhow::anyhow!(
                        "acknowledgment of netlink message {sequence} carries error code {code}, which is not a negated errno"
                    )));
                };
                if sequence == fence {
                    return Err(Failure::Other(anyhow::anyhow!(
                        "the fence after netlink batch {}..={} failed: {}",
                        self.first,
                        self.last,
                        std::io::Error::from_raw_os_error(errno)
                    )));
                }
                let index =
                    classify(sequence, self.first, self.deletes, errno).map_err(Failure::Other)?;
                self.absent.insert(index);
                continue;
            }
            if sequence == self.last - 1 {
                self.inserted = true;
            } else if sequence == fence {
                if !self.inserted && self.absent.is_empty() {
                    return Err(Failure::Other(anyhow::anyhow!(
                        "netlink batch {}..={} reached its fence without its insert's acknowledgment",
                        self.first,
                        self.last
                    )));
                }
                fenced = true;
            }
        }
        if !fenced {
            return Ok(false);
        }
        if !self.absent.is_empty() {
            return Err(Failure::Absent(std::mem::take(&mut self.absent)));
        }
        Ok(true)
    }
}

/// The index of a delete answered with `ENOENT`: its element expired between
/// the resolver deciding it was still held and the kernel committing.
/// Everything else is a failure the resolver cannot reason its way out of.
fn classify(sequence: u32, first: u32, deletes: usize, errno: i32) -> anyhow::Result<usize> {
    let index = sequence.saturating_sub(first + 1) as usize;
    let is_delete = sequence > first && index < deletes;

    if is_delete && errno == libc::ENOENT {
        return Ok(index);
    }
    Err(anyhow::anyhow!(
        "netlink message {sequence} of the batch failed: {}",
        std::io::Error::from_raw_os_error(errno)
    ))
}

impl SetWriter for Netlink {
    fn apply(&mut self, deletes: &[Ipv4Addr], adds: &[Element]) -> anyhow::Result<()> {
        // One deadline for the call, so a resend cannot extend it.
        let deadline = Instant::now() + TIMEOUT;

        retry(deletes, |deletes| self.transaction(deletes, adds, deadline))
    }
}

/// Elements can expire between `resolver::plan` and kernel processing. Drain
/// every reply through the fence before retrying without all absent deletes.
/// Each attempt drops at least one, bounding attempts by the delete count plus one.
fn retry(
    deletes: &[Ipv4Addr],
    mut attempt: impl FnMut(&[Ipv4Addr]) -> Result<(), Failure>,
) -> anyhow::Result<()> {
    let mut deletes = deletes.to_vec();

    for _ in 0..=deletes.len() {
        match attempt(&deletes) {
            Ok(()) => return Ok(()),
            Err(Failure::Absent(absent)) => {
                deletes = deletes
                    .into_iter()
                    .enumerate()
                    .filter(|(index, _)| !absent.contains(index))
                    .map(|(_, delete)| delete)
                    .collect();
            }
            Err(Failure::Other(error)) => return Err(error),
        }
    }
    anyhow::bail!("a netlink batch could not be committed after dropping every stale delete")
}

/// The batch, as bytes. Pure, so the encoding is snapshot-tested without a
/// kernel. Sequences run `first` (the batch marker) through
/// `first + deletes.len() + 2` (the closing marker). Only the insert asks
/// for an acknowledgment: a failure is answered regardless.
pub fn encode_batch(
    table: &str,
    set: &str,
    deletes: &[Ipv4Addr],
    adds: &[Element],
    first: u32,
) -> Vec<u8> {
    const REQUEST: u16 = netlink_packet_core::NLM_F_REQUEST;

    // The markers name the nfnetlink subsystem the batch is for. The messages
    // between them name the table's family.
    let marker = netlink_packet_netfilter::NetfilterHeader::new(
        netlink_packet_netfilter::NetfilterProtoFamily::Unspec,
        0,
        u8::from(netlink_packet_netfilter::NetfilterSubsystem::NfTables).into(),
    );
    let inet = netlink_packet_netfilter::NetfilterHeader::new(
        netlink_packet_netfilter::NetfilterProtoFamily::Inet,
        0,
        0,
    );
    let list = |elements: Vec<nftables::ListAttribute<nftables::SetElementAttribute>>| {
        nftables::SetElementMessage {
            attributes: vec![
                nftables::SetElementList::Table(table.to_string()),
                nftables::SetElementList::Set(set.to_string()),
                nftables::SetElementList::Elements(elements),
            ],
        }
    };
    let key = |address: Ipv4Addr| {
        nftables::SetElementAttribute::Key(nftables::DataAttribute::Value(
            address.octets().to_vec(),
        ))
    };

    let mut messages = vec![(
        netlink_packet_netfilter::NetfilterMessage::new(
            marker.clone(),
            netlink_packet_netfilter::none::ControlMessage::BatchBegin,
        ),
        REQUEST,
    )];
    for address in deletes {
        let element = nftables::ListAttribute::Element(vec![key(*address)]);
        messages.push((
            netlink_packet_netfilter::NetfilterMessage::new(
                inet.clone(),
                nftables::NfTablesMessage::DeleteSetElement(list(vec![element])),
            ),
            REQUEST,
        ));
    }
    let elements = adds
        .iter()
        .map(|add| {
            let milliseconds = add.timeout.as_millis().min(u64::MAX as u128) as u64;
            nftables::ListAttribute::Element(vec![
                key(add.address),
                nftables::SetElementAttribute::Timeout(milliseconds),
            ])
        })
        .collect();
    messages.push((
        netlink_packet_netfilter::NetfilterMessage::new(
            inet,
            nftables::NfTablesMessage::NewSetElement(list(elements)),
        ),
        REQUEST
            | netlink_packet_core::NLM_F_ACK
            | netlink_packet_core::NLM_F_CREATE
            | netlink_packet_core::NLM_F_EXCL,
    ));
    messages.push((
        netlink_packet_netfilter::NetfilterMessage::new(
            marker,
            netlink_packet_netfilter::none::ControlMessage::BatchEnd,
        ),
        REQUEST,
    ));

    let mut batch = Vec::new();
    for ((message, flags), sequence) in messages.into_iter().zip(first..) {
        let mut message = netlink_packet_core::NetlinkMessage::from(message);
        message.header.flags = flags;
        message.header.sequence_number = sequence;
        message.finalize();

        // `finalize` records the unpadded length in the header. Each message
        // starts on a 4-byte boundary.
        let start = batch.len();
        let length = message.buffer_len();
        batch.resize(start + length.next_multiple_of(4), 0);
        message.serialize(&mut batch[start..start + length]);
    }
    batch
}

/// `netlink_rcv_skb` acknowledges this control message without calling nf_tables.
pub fn encode_fence(sequence: u32) -> Vec<u8> {
    let mut message =
        netlink_packet_core::NetlinkMessage::<netlink_packet_netfilter::NetfilterMessage>::new(
            netlink_packet_core::NetlinkHeader::default(),
            netlink_packet_core::NetlinkPayload::Noop,
        );
    message.header.flags = netlink_packet_core::NLM_F_REQUEST | netlink_packet_core::NLM_F_ACK;
    message.header.sequence_number = sequence;
    message.finalize();

    let mut fence = vec![0; message.buffer_len()];
    message.serialize(&mut fence);
    fence
}

#[cfg(test)]
mod tests {
    use netlink_packet_netfilter::nftables;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    fn address(last: u8) -> Ipv4Addr {
        Ipv4Addr::new(93, 184, 216, last)
    }

    /// The batch the resolver sends to refresh one address and add another.
    /// The kernel accepted this exact shape; the snapshot is here so a change
    /// to a field, a flag or the padding has to be deliberate.
    #[test]
    fn batch_encoding() {
        let adds = [
            super::Element {
                address: address(34),
                timeout: Duration::from_secs(300),
            },
            super::Element {
                address: address(35),
                timeout: Duration::from_millis(1_500),
            },
        ];
        let batch = super::encode_batch(super::TABLE, super::SET, &[address(34)], &adds, 7);
        insta::assert_snapshot!(annotate(&batch));
    }

    #[test]
    fn batch_encoding_without_deletes() {
        let adds = [super::Element {
            address: address(34),
            timeout: Duration::from_secs(90),
        }];
        let batch = super::encode_batch(super::TABLE, super::SET, &[], &adds, 1);
        insta::assert_snapshot!(annotate(&batch));
    }

    /// The fence: a bare 16-byte `nlmsghdr` of type `NLMSG_NOOP` (1), with
    /// `NLM_F_REQUEST|NLM_F_ACK` (5) and its sequence (12).
    #[test]
    fn fence_encoding() {
        insta::assert_snapshot!(hex(&super::encode_fence(12)), @"10000000010005000c00000000000000");
    }

    #[test]
    fn batch_encoding_edges() {
        let adds = [
            super::Element {
                address: address(34),
                timeout: Duration::from_micros(1_500_999),
            },
            super::Element {
                address: address(35),
                timeout: Duration::from_millis(0x0102_0304_0506_0708),
            },
            super::Element {
                address: address(36),
                timeout: Duration::MAX,
            },
        ];
        let deletes = [address(34), address(35)];
        let batch = super::encode_batch(super::TABLE, super::SET, &deletes, &adds, 7);
        insta::assert_snapshot!(annotate(&batch));
    }

    /// This module names the table and set as strings; the ruleset renders
    /// them. Neither can move without the other.
    #[test]
    fn names_match_the_ruleset() {
        let policy = crate::policy::tests::policy(&[], &[], None);
        let text = crate::ruleset::render(&policy, &[], &[]);

        assert!(
            text.contains(&format!("table inet {} {{", super::TABLE)),
            "the ruleset does not declare table inet {}",
            super::TABLE
        );
        assert!(
            text.contains(&format!("set {} {{", super::SET)),
            "the ruleset does not declare set {}",
            super::SET
        );
    }

    /// Only an ENOENT against a delete is recoverable, and it has to name the
    /// right one.
    #[test]
    fn errors_are_classified() {
        let table = [
            (8, libc::ENOENT, "delete 0 missing"),
            (9, libc::ENOENT, "delete 1 missing"),
            (10, libc::ENOENT, "the insert, not a delete"),
            (8, libc::EEXIST, "a delete failing some other way"),
            (7, libc::EPERM, "the batch marker"),
        ];
        let mut rendered = String::new();

        for (sequence, errno, label) in table {
            let outcome = match super::classify(sequence, 7, 2, errno) {
                Ok(index) => format!("absent delete {index}"),
                Err(error) => format!("{error:#}"),
            };
            rendered.push_str(&format!(
                "# {label}\nseq={sequence} errno={errno} -> {outcome}\n\n"
            ));
        }
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn a_stale_delete_is_dropped_and_the_batch_resent() {
        let held = [address(1), address(2), address(3)];
        let mut attempts: Vec<Vec<Ipv4Addr>> = Vec::new();

        super::retry(&held, |deletes| {
            attempts.push(deletes.to_vec());
            // The second address is gone; everything else commits.
            match deletes.iter().position(|a| *a == address(2)) {
                Some(index) => Err(super::Failure::Absent([index].into())),
                None => Ok(()),
            }
        })
        .expect("the retry drops the stale delete and commits");

        assert_eq!(
            attempts,
            vec![
                vec![address(1), address(2), address(3)],
                vec![address(1), address(3)],
            ]
        );
    }

    /// An error burst must cost one resend.
    #[test]
    fn stale_deletes_are_dropped_together() {
        let deletes: Vec<Ipv4Addr> = (1..=6).map(address).collect();
        let held = [address(2), address(5)];
        let mut attempts: Vec<Vec<Ipv4Addr>> = Vec::new();

        super::retry(&deletes, |deletes| {
            attempts.push(deletes.to_vec());
            let absent: std::collections::BTreeSet<usize> = deletes
                .iter()
                .enumerate()
                .filter(|(_, delete)| !held.contains(delete))
                .map(|(index, _)| index)
                .collect();
            match absent.is_empty() {
                true => Ok(()),
                false => Err(super::Failure::Absent(absent)),
            }
        })
        .expect("the retry drops every stale delete and commits");

        assert_eq!(attempts, vec![deletes.clone(), held.to_vec()]);
    }

    #[test]
    fn other_failures_are_not_retried() {
        let mut attempts = 0;
        let error = super::retry(&[address(1)], |_| {
            attempts += 1;
            Err(super::Failure::Other(anyhow::anyhow!("EPERM")))
        })
        .expect_err("a permission failure is not recoverable");

        assert_eq!(attempts, 1);
        assert_eq!(error.to_string(), "EPERM");
    }

    /// The worst case the loop has to cover: every held element expired, one
    /// between each attempt and the next, so the batch is resent once per
    /// delete before committing with none.
    #[test]
    fn every_delete_can_be_stale() {
        let held = [address(1), address(2)];
        let mut attempts = 0;

        super::retry(&held, |deletes| {
            attempts += 1;
            match deletes.is_empty() {
                true => Ok(()),
                false => Err(super::Failure::Absent([0].into())),
            }
        })
        .expect("the batch commits once the last stale delete is dropped");

        assert_eq!(
            attempts, 3,
            "one attempt per delete, plus the one that commits"
        );
    }

    /// A batch of two deletes runs from sequence 7 (the opening marker) to 11
    /// (the closing one), with its insert at 10 and its fence at 12. Each
    /// case is the datagrams one receive sees, in the order Linux 6.8 queues
    /// them: a batch's replies, then its fence's.
    #[test]
    fn acknowledgments() {
        const NLMSG_ERROR: u16 = 2;
        const NLMSG_DONE: u16 = 3;
        const NEWSETELEM: u16 = 10 << 8 | 12;
        let replies = |sequences: &[u32]| -> Vec<u8> {
            sequences
                .iter()
                .flat_map(|sequence| reply(*sequence, 0))
                .collect()
        };
        let cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
            (
                "a commit: the insert's acknowledgment, then the fence's",
                vec![reply(10, 0), reply(12, 0)],
            ),
            ("coalesced", vec![replies(&[10, 12])]),
            (
                "every message acknowledged, as a kernel asked to might",
                vec![replies(&[7, 8, 9, 10, 11, 12])],
            ),
            ("the insert's acknowledgment alone", vec![reply(10, 0)]),
            (
                "the fence's alone: an abort which answered nothing",
                vec![reply(12, 0)],
            ),
            (
                "the fence's ahead of the insert's",
                vec![replies(&[12, 10])],
            ),
            (
                "the insert's and the fence's, then an error of the batch, in one datagram",
                vec![[replies(&[10, 12]), reply(8, -libc::ENOENT)].concat()],
            ),
            (
                "the insert's and the fence's, then a malformed message, in one datagram",
                vec![[replies(&[10, 12]), vec![0; 8]].concat()],
            ),
            (
                "a failed commit: the opening marker's error, then the queued inner acknowledgment",
                vec![reply(7, -libc::ENOMEM), reply(10, 0), reply(12, 0)],
            ),
            (
                "a failed header: EPERM on the opening marker, and on the fence",
                vec![reply(7, -libc::EPERM), reply(12, -libc::EPERM)],
            ),
            (
                "a failure on the closing marker",
                vec![reply(10, 0), reply(11, -libc::EINVAL), reply(12, 0)],
            ),
            (
                "a failed fence",
                vec![reply(10, 0), reply(12, -libc::ENOMEM)],
            ),
            (
                "replies to abandoned batches, and messages that are not acknowledgments",
                vec![
                    [
                        reply(6, -libc::ENOENT),
                        reply(13, -libc::EPERM),
                        raw(NLMSG_ERROR, 5, &[], 16),
                        raw(NEWSETELEM, 8, &[0xff; 4], 20),
                        raw(NLMSG_DONE, 11, &[], 16),
                    ]
                    .concat(),
                    replies(&[10, 12]),
                ],
            ),
            (
                "ENOENT on delete 0",
                vec![reply(8, -libc::ENOENT), reply(10, 0), reply(12, 0)],
            ),
            (
                "ENOENT on delete 1",
                vec![[reply(9, -libc::ENOENT), replies(&[10, 12])].concat()],
            ),
            (
                "ENOENT on both deletes, gathered through the fence",
                vec![
                    reply(8, -libc::ENOENT),
                    reply(9, -libc::ENOENT),
                    reply(10, 0),
                    reply(12, 0),
                ],
            ),
            (
                "ENOENT on a delete, then EEXIST on the insert",
                vec![reply(8, -libc::ENOENT), reply(10, -libc::EEXIST)],
            ),
            ("EEXIST on a delete", vec![reply(8, -libc::EEXIST)]),
            ("ENOENT on the insert", vec![reply(10, -libc::ENOENT)]),
            ("EEXIST on the insert", vec![reply(10, -libc::EEXIST)]),
            ("error code i32::MIN", vec![reply(8, i32::MIN)]),
            ("a positive error code", vec![reply(8, libc::ENOENT)]),
            ("a header cut short", vec![reply(7, 0)[..12].to_vec()]),
            (
                "a length below the header's",
                vec![raw(NLMSG_ERROR, 7, &[0; 20], 8)],
            ),
            (
                "a length beyond the datagram",
                vec![raw(NLMSG_ERROR, 7, &[0; 20], 64)],
            ),
            ("no error code", vec![raw(NLMSG_ERROR, 7, &[], 16)]),
            (
                "3 bytes of error code",
                vec![raw(NLMSG_ERROR, 7, &[0; 4], 19)],
            ),
            ("trailing bytes", vec![[reply(7, 0), vec![0; 8]].concat()]),
            (
                "an unpadded last message",
                vec![raw(NLMSG_ERROR, 7, &[0; 21], 37)],
            ),
        ];
        let mut rendered = String::new();

        for (label, datagrams) in cases {
            let mut acknowledgments = super::Acknowledgments::new(7, 11, 2);
            rendered.push_str(&format!("# {label}\n"));

            // A failure ends the transaction, so nothing after it is read.
            for datagram in datagrams {
                let (outcome, failed) = match acknowledgments.receive(&datagram) {
                    Ok(false) => ("waiting".to_string(), false),
                    Ok(true) => ("committed".to_string(), false),
                    Err(super::Failure::Absent(absent)) => {
                        (format!("absent deletes {absent:?}"), true)
                    }
                    Err(super::Failure::Other(error)) => (format!("{error:#}"), true),
                };
                rendered.push_str(&format!("{} bytes -> {outcome}\n", datagram.len()));
                if failed {
                    break;
                }
            }
            rendered.push('\n');
        }
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn malformed_datagrams_do_not_panic() {
        let datagram = [reply(7, 0), reply(8, -libc::ENOENT), reply(11, 0)].concat();

        for end in 0..=datagram.len() {
            let _ = super::Acknowledgments::new(7, 11, 2).receive(&datagram[..end]);
        }
        for length in 0..=64u32 {
            let mut datagram = datagram.clone();
            datagram[..4].copy_from_slice(&length.to_ne_bytes());
            let _ = super::Acknowledgments::new(7, 11, 2).receive(&datagram);
        }
    }

    /// Replies shaped as Linux 6.8 sends them, through `retry`: each in its
    /// own datagram, an errno for each failed message, the insert's
    /// acknowledgment, none for the markers, then the fence's. Replies left
    /// unread by a failed attempt are read by the next, as on the socket.
    #[test]
    fn kernel_replies_drive_the_retry() {
        let cases = [
            ("one of two deletes expired", vec![address(1)], 0),
            ("the set is gone", vec![], -libc::ENOENT),
            (
                "an insert of a live element",
                vec![address(1), address(2)],
                -libc::EEXIST,
            ),
        ];
        let mut rendered = String::new();

        for (label, held, insert) in cases {
            let mut queue = std::collections::VecDeque::new();
            let mut first = 1;
            rendered.push_str(&format!("# {label}\n"));

            let outcome = super::retry(&[address(1), address(2)], |deletes| {
                let (batch, last) = (first, first + deletes.len() as u32 + 2);
                first = last + 2;
                rendered.push_str(&format!("attempt {batch}..={last} deletes {deletes:?}\n"));

                for (sequence, delete) in (batch + 1..).zip(deletes) {
                    if !held.contains(delete) {
                        queue.push_back(reply(sequence, -libc::ENOENT));
                    }
                }
                queue.push_back(reply(last - 1, insert));
                queue.push_back(reply(last + 1, 0));

                let mut acknowledgments = super::Acknowledgments::new(batch, last, deletes.len());
                while let Some(datagram) = queue.pop_front() {
                    if acknowledgments.receive(&datagram)? {
                        return Ok(());
                    }
                }
                Err(super::Failure::Other(anyhow::anyhow!("no commit")))
            });
            match outcome {
                Ok(()) => rendered.push_str("-> committed\n\n"),
                Err(error) => rendered.push_str(&format!("-> {error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(rendered);
    }

    /// At the ruleset's capacity, the insert's element list still fits its
    /// 16-bit attribute length, so every element decodes back.
    #[test]
    fn batch_encoding_at_capacity() {
        let adds: Vec<super::Element> = (0..crate::ruleset::RESOLVED_SIZE as u32)
            .map(|index| super::Element {
                address: Ipv4Addr::from(0x0a00_0000 + index),
                timeout: Duration::from_secs(60),
            })
            .collect();
        let deletes: Vec<Ipv4Addr> = adds.iter().map(|add| add.address).collect();
        let batch = super::encode_batch(super::TABLE, super::SET, &deletes, &adds, 1);

        let mut messages = Vec::new();
        let mut offset = 0;
        while offset < batch.len() {
            let message = netlink_packet_core::NetlinkMessage::<
                netlink_packet_netfilter::NetfilterMessage,
            >::deserialize(&batch[offset..])
            .unwrap();
            offset += (message.header.length as usize).next_multiple_of(4);
            messages.push(message);
        }
        assert_eq!(messages.len(), deletes.len() + 3);
        // A failed insert is answered with its own header, an errno, and the
        // whole insert, which has to fit one receive.
        let echoed = 16 + 4 + messages[deletes.len() + 1].header.length as usize;
        assert!(
            echoed <= super::REPLY_DATAGRAM,
            "a failed insert's reply of {echoed} bytes overflows {}",
            super::REPLY_DATAGRAM
        );

        let netlink_packet_core::NetlinkPayload::InnerMessage(insert) =
            &messages[deletes.len() + 1].payload
        else {
            panic!("the insert is not an inner message");
        };
        let netlink_packet_netfilter::NetfilterMessageInner::NfTables(
            nftables::NfTablesMessage::NewSetElement(insert),
        ) = &insert.inner
        else {
            panic!("the insert is not NFT_MSG_NEWSETELEM");
        };
        let nftables::SetElementList::Elements(elements) = &insert.attributes[2] else {
            panic!("the insert's third attribute is not its elements");
        };
        assert_eq!(elements.len(), crate::ruleset::RESOLVED_SIZE);
        assert_eq!(
            elements.last(),
            Some(&nftables::ListAttribute::Element(vec![
                nftables::SetElementAttribute::Key(nftables::DataAttribute::Value(vec![
                    10, 0, 3, 255
                ])),
                nftables::SetElementAttribute::Timeout(60_000),
            ]))
        );
    }

    /// Exercise commits and retries with unread replies from earlier attempts.
    /// Run in a fresh namespace:
    /// `sudo -n unshare --net -- <test binary> --ignored --exact resolver::nftset::tests::real_kernel_transactions`
    #[test]
    #[ignore = "needs root in a private network namespace"]
    fn real_kernel_transactions() {
        private_network_namespace();
        let policy = crate::policy::tests::policy(&[], &[], None);
        nft(&["-f", "-"], &crate::ruleset::render(&policy, &[], &[]));

        let element = |last, seconds| super::Element {
            address: address(last),
            timeout: Duration::from_secs(seconds),
        };
        let cases: Vec<(&str, Vec<Ipv4Addr>, Vec<super::Element>)> = vec![
            ("insert two", vec![], vec![element(1, 300), element(2, 300)]),
            ("refresh one", vec![address(1)], vec![element(1, 600)]),
            (
                "refresh one the set does not hold",
                vec![address(3)],
                vec![element(3, 900)],
            ),
            ("insert one the set holds", vec![], vec![element(2, 1200)]),
            (
                "refresh after that failure",
                vec![address(2)],
                vec![element(2, 1500)],
            ),
        ];
        let mut netlink = super::Netlink::open().unwrap();
        let mut rendered = String::new();

        for (label, deletes, adds) in cases {
            let outcome = match super::SetWriter::apply(&mut netlink, &deletes, &adds) {
                Ok(()) => "committed".to_string(),
                Err(error) => format!("{error:#}"),
            };
            rendered.push_str(&format!("{label}: {outcome}\n  holds {}\n", held()));
        }
        nft(&["delete", "table", "inet", super::TABLE], "");
        let error = super::SetWriter::apply(&mut netlink, &[], &[element(4, 300)]).unwrap_err();
        rendered.push_str(&format!("insert into a deleted table: {error:#}\n"));
        insta::assert_snapshot!(rendered, @"
        insert two: committed
          holds 93.184.216.1/300s 93.184.216.2/300s
        refresh one: committed
          holds 93.184.216.1/600s 93.184.216.2/300s
        refresh one the set does not hold: committed
          holds 93.184.216.1/600s 93.184.216.2/300s 93.184.216.3/900s
        insert one the set holds: netlink message 20 of the batch failed: File exists (os error 17)
          holds 93.184.216.1/600s 93.184.216.2/300s 93.184.216.3/900s
        refresh after that failure: committed
          holds 93.184.216.1/600s 93.184.216.2/1500s 93.184.216.3/900s
        insert into a deleted table: netlink message 29 of the batch failed: No such file or directory (os error 2)
        ");
    }

    /// Without CAP_NET_ADMIN, `open` is refused its receive buffer. A batch
    /// sent anyway is refused by nfnetlink before nf_tables sees it, and
    /// answered against its opening marker. Run as root without CAP_NET_ADMIN:
    /// `sudo -n unshare --net -- setpriv --bounding-set=-net_admin --inh-caps=-net_admin <test binary> --ignored --exact resolver::nftset::tests::real_kernel_refuses_a_header`
    #[test]
    #[ignore = "needs a private network namespace, without CAP_NET_ADMIN"]
    fn real_kernel_refuses_a_header() {
        private_network_namespace();
        let refused = super::Netlink::open()
            .err()
            .expect("open without CAP_NET_ADMIN is refused");
        let add = super::Element {
            address: address(1),
            timeout: Duration::from_secs(300),
        };
        let error = super::SetWriter::apply(&mut super::Netlink::connect().unwrap(), &[], &[add])
            .unwrap_err();
        insta::assert_snapshot!(format!("open: {refused:#}\nbatch: {error:#}"), @"
        open: sizing the netlink receive buffer: Operation not permitted (os error 1)
        batch: netlink message 1 of the batch failed: Operation not permitted (os error 1)
        ");
    }

    /// Full-capacity refreshes must survive error bursts and preserve atomicity.
    /// Run in a fresh namespace:
    /// `sudo -n unshare --net -- <test binary> --ignored --exact resolver::nftset::tests::real_kernel_at_capacity`
    #[test]
    #[ignore = "needs root in a private network namespace"]
    fn real_kernel_at_capacity() {
        private_network_namespace();
        let policy = crate::policy::tests::policy(&[], &[], None);
        nft(&["-f", "-"], &crate::ruleset::render(&policy, &[], &[]));

        // Distinct addresses of 198.18.0.0/15, the benchmarking range.
        let addresses: Vec<Ipv4Addr> = (0..crate::ruleset::RESOLVED_SIZE as u32)
            .map(|index| Ipv4Addr::from(0xc612_0000 + index))
            .collect();
        let all = |seconds| -> Vec<super::Element> {
            addresses
                .iter()
                .map(|address| super::Element {
                    address: *address,
                    timeout: Duration::from_secs(seconds),
                })
                .collect()
        };
        let flush = || nft(&["flush", "set", "inet", super::TABLE, super::SET], "");

        // Ordinary sizing varies with rmem_max. Production forces and
        // verifies the required budget.
        let ordinary = super::Netlink::connect().unwrap();
        rustix::net::sockopt::set_socket_recv_buffer_size(&ordinary.socket, super::RECEIVE_BUFFER)
            .unwrap();
        eprintln!(
            "an ordinary request for {} bytes got {}, with net.core.rmem_max {}",
            super::RECEIVE_BUFFER,
            rustix::net::sockopt::socket_recv_buffer_size(&ordinary.socket).unwrap(),
            std::fs::read_to_string("/proc/sys/net/core/rmem_max")
                .unwrap()
                .trim(),
        );
        std::mem::drop(ordinary);

        let mut netlink = super::Netlink::open().unwrap();
        let rendered = &mut format!(
            "effective receive buffer: {} bytes\n",
            rustix::net::sockopt::socket_recv_buffer_size(&netlink.socket).unwrap()
        );
        fn apply(
            netlink: &mut super::Netlink,
            rendered: &mut String,
            label: &str,
            deletes: &[Ipv4Addr],
            adds: &[super::Element],
        ) {
            let outcome = match super::SetWriter::apply(netlink, deletes, adds) {
                Ok(()) => "committed".to_string(),
                Err(error) => format!("{error:#}"),
            };
            rendered.push_str(&format!("{label}: {outcome}\n  holds {}\n", held_summary()));
        }
        let n = &mut netlink;

        apply(n, rendered, "insert all", &[], &all(300));
        apply(n, rendered, "refresh all", &addresses, &all(600));
        apply(n, rendered, "refresh all again", &addresses, &all(900));
        apply(
            n,
            rendered,
            "refresh all but delete one fewer, so one insert meets a live element",
            &addresses[1..],
            &all(1200),
        );

        // Every reply to a refresh whose deletes are all absent, queued at
        // once before anything is read: what each takes of the receive buffer.
        flush();
        let first = n.sequence;
        let last = first + addresses.len() as u32 + 2;
        n.sequence = last + 2;
        for datagram in [
            super::encode_batch(super::TABLE, super::SET, &addresses, &all(1500), first),
            super::encode_fence(last + 1),
        ] {
            rustix::net::send(&n.socket, &datagram, rustix::net::SendFlags::empty()).unwrap();
        }
        let (queued, replies) = (queued_bytes(&n.socket), addresses.len() + 2);
        eprintln!(
            "{replies} replies queued {queued} bytes, {} each",
            queued / replies
        );
        assert!(
            queued <= replies * super::REPLY_BYTES,
            "{replies} replies queued {queued} bytes, beyond {} each",
            super::REPLY_BYTES
        );
        let outcome = n.read_acknowledgments(
            super::Acknowledgments::new(first, last, addresses.len()),
            std::time::Instant::now() + super::TIMEOUT,
        );
        let Err(super::Failure::Absent(absent)) = outcome else {
            panic!("every delete of the refresh is absent");
        };
        assert_eq!(absent.len(), addresses.len());

        apply(
            n,
            rendered,
            "refresh all, none held",
            &addresses,
            &all(1500),
        );
        // Expired entries count toward size until GC. Half capacity leaves
        // room for replacements without waiting for reclamation:
        // <https://github.com/torvalds/linux/blob/v6.8/net/netfilter/nft_set_hash.c#L319-L392>.
        flush();
        let expiring = &addresses[..addresses.len() / 2];
        apply(
            n,
            rendered,
            "insert half to expire",
            &[],
            &all(2)[..expiring.len()],
        );
        std::thread::sleep(Duration::from_secs(3));
        apply(
            n,
            rendered,
            "refresh that half, every one expired",
            expiring,
            &all(1800)[..expiring.len()],
        );

        flush();
        let half: Vec<super::Element> = all(300).into_iter().step_by(2).collect();
        apply(n, rendered, "insert every other", &[], &half);
        apply(
            n,
            rendered,
            "refresh all, half held",
            &addresses,
            &all(2100),
        );
        insta::assert_snapshot!(rendered, @r#"
        effective receive buffer: 4337664 bytes
        insert all: committed
          holds 1024 distinct, timeouts {"300s"}, from Some(198.18.0.0) to Some(198.18.3.255)
        refresh all: committed
          holds 1024 distinct, timeouts {"600s"}, from Some(198.18.0.0) to Some(198.18.3.255)
        refresh all again: committed
          holds 1024 distinct, timeouts {"900s"}, from Some(198.18.0.0) to Some(198.18.3.255)
        refresh all but delete one fewer, so one insert meets a live element: netlink message 3085 of the batch failed: File exists (os error 17)
          holds 1024 distinct, timeouts {"900s"}, from Some(198.18.0.0) to Some(198.18.3.255)
        refresh all, none held: committed
          holds 1024 distinct, timeouts {"1500s"}, from Some(198.18.0.0) to Some(198.18.3.255)
        insert half to expire: committed
          holds 512 distinct, timeouts {"2s"}, from Some(198.18.0.0) to Some(198.18.1.255)
        refresh that half, every one expired: committed
          holds 512 distinct, timeouts {"1800s"}, from Some(198.18.0.0) to Some(198.18.1.255)
        insert every other: committed
          holds 512 distinct, timeouts {"300s"}, from Some(198.18.0.0) to Some(198.18.3.254)
        refresh all, half held: committed
          holds 1024 distinct, timeouts {"2100s"}, from Some(198.18.0.0) to Some(198.18.3.255)
        "#);
    }

    /// The bytes queued for `socket` to receive, as /proc/net/netlink lists
    /// them: the `truesize` of every reply waiting there.
    fn queued_bytes(socket: &std::os::fd::OwnedFd) -> usize {
        let inode = rustix::fs::fstat(socket).unwrap().st_ino.to_string();
        let table = std::fs::read_to_string("/proc/net/netlink").unwrap();
        // sk Eth Pid Groups Rmem Wmem Dump Locks Drops Inode
        table
            .lines()
            .skip(1)
            .find_map(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                (fields.get(9) == Some(&inode.as_str())).then(|| fields[4].parse().unwrap())
            })
            .expect("/proc/net/netlink lists the socket")
    }

    /// The set's live elements, summarized: how many, their timeouts, and
    /// the lowest and highest address.
    fn held_summary() -> String {
        let listing: serde_json::Value = serde_json::from_str(&nft(
            &["-j", "list", "set", "inet", super::TABLE, super::SET],
            "",
        ))
        .unwrap();
        let set = listing["nftables"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|entry| entry.get("set"))
            .expect("the listing has the set");
        let elements = set["elem"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default();
        let addresses: std::collections::BTreeSet<Ipv4Addr> = elements
            .iter()
            .map(|element| element["elem"]["val"].as_str().unwrap().parse().unwrap())
            .collect();
        let timeouts: std::collections::BTreeSet<String> = elements
            .iter()
            .map(|element| format!("{}s", element["elem"]["timeout"]))
            .collect();
        format!(
            "{} distinct, timeouts {timeouts:?}, from {:?} to {:?}",
            addresses.len(),
            addresses.first(),
            addresses.last()
        )
    }

    /// The real-kernel tests change a ruleset, so they run only in a network
    /// namespace other than init's.
    fn private_network_namespace() {
        use std::os::unix::fs::MetadataExt;

        let ours = std::fs::metadata("/proc/self/ns/net").unwrap().ino();
        let init = std::fs::metadata("/proc/1/ns/net")
            .expect("reading init's network namespace takes root")
            .ino();
        assert_ne!(ours, init, "run this under `unshare --net`");
    }

    fn nft(args: &[&str], input: &str) -> String {
        use std::io::Write;

        let mut child = std::process::Command::new("nft")
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("running nft");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "nft {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    /// The set's elements, as `nft -j` lists them: address and timeout.
    fn held() -> String {
        let listing: serde_json::Value = serde_json::from_str(&nft(
            &["-j", "list", "set", "inet", super::TABLE, super::SET],
            "",
        ))
        .unwrap();
        let set = listing["nftables"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|entry| entry.get("set"))
            .expect("the listing has the set");
        let mut elements: Vec<String> = set["elem"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .map(|element| {
                let element = &element["elem"];
                format!(
                    "{}/{}s",
                    element["val"].as_str().unwrap(),
                    element["timeout"]
                )
            })
            .collect();
        elements.sort();
        elements.join(" ")
    }

    /// The batch, message by message and attribute by attribute, with the
    /// bytes beside each field. Numbers are the kernel UAPI headers' own
    /// (`linux/netlink.h`, `linux/netfilter/nfnetlink.h` and
    /// `linux/netfilter/nf_tables.h`), so this reads the bytes independently
    /// of the library that wrote them.
    fn annotate(batch: &[u8]) -> String {
        const NLMSG_HEADER_LEN: usize = 16;
        let mut rendered = String::new();
        let mut offset = 0;

        while offset + NLMSG_HEADER_LEN <= batch.len() {
            let length = u32::from_ne_bytes(batch[offset..offset + 4].try_into().unwrap()) as usize;
            let kind = u16::from_ne_bytes(batch[offset + 4..offset + 6].try_into().unwrap());
            let flags = u16::from_ne_bytes(batch[offset + 6..offset + 8].try_into().unwrap());
            let sequence = u32::from_ne_bytes(batch[offset + 8..offset + 12].try_into().unwrap());

            rendered.push_str(&format!(
                "byte {offset}: nlmsghdr len={length} type={} flags={} seq={sequence} pid=0\n",
                message_name(kind),
                flag_names(flags),
            ));
            let body = &batch[offset + NLMSG_HEADER_LEN..offset + length];
            rendered.push_str(&format!(
                "     nfgenmsg family={} version={} res_id={}\n",
                body[0],
                body[1],
                u16::from_be_bytes([body[2], body[3]]),
            ));
            attributes(&body[4..], 0, &mut rendered);
            offset += length.next_multiple_of(4);
        }
        rendered.push_str(&format!("\n{} bytes\n{}\n", batch.len(), hex(batch)));
        rendered
    }

    fn attributes(mut payload: &[u8], level: usize, rendered: &mut String) {
        const NLA_F_NESTED: u16 = 0x8000;
        let indent = 5 + level * 2;

        while payload.len() >= 4 {
            let length = u16::from_ne_bytes(payload[0..2].try_into().unwrap()) as usize;
            let kind = u16::from_ne_bytes(payload[2..4].try_into().unwrap());
            let nested = kind & NLA_F_NESTED != 0;
            let value = &payload[4..length];

            rendered.push_str(&format!(
                "{:indent$}{} len={length}{}\n",
                "",
                attribute_name(kind & !NLA_F_NESTED, level),
                if nested {
                    String::new()
                } else {
                    format!(" {}", hex(value))
                },
            ));
            if nested {
                attributes(value, level + 1, rendered);
            }
            payload = &payload[length.next_multiple_of(4).min(payload.len())..];
        }
    }

    fn message_name(kind: u16) -> String {
        match kind {
            16 => "NFNL_MSG_BATCH_BEGIN".to_string(),
            17 => "NFNL_MSG_BATCH_END".to_string(),
            _ => {
                let name = match kind & 0xFF {
                    12 => "NFT_MSG_NEWSETELEM",
                    14 => "NFT_MSG_DELSETELEM",
                    _ => "?",
                };
                format!("{name}(subsys={})", kind >> 8)
            }
        }
    }

    /// Attribute numbers repeat between nesting levels - the table, the list
    /// element and the key are all attribute 1 - so the depth decides which
    /// name applies.
    fn attribute_name(kind: u16, level: usize) -> &'static str {
        match (level, kind) {
            (0, 1) => "NFTA_SET_ELEM_LIST_TABLE",
            (0, 2) => "NFTA_SET_ELEM_LIST_SET",
            (0, 3) => "NFTA_SET_ELEM_LIST_ELEMENTS",
            (1, 1) => "NFTA_LIST_ELEM",
            (2, 1) => "NFTA_SET_ELEM_KEY",
            (2, 4) => "NFTA_SET_ELEM_TIMEOUT (be64 ms)",
            (3, 1) => "NFTA_DATA_VALUE",
            _ => "?",
        }
    }

    fn flag_names(flags: u16) -> String {
        let mut names = Vec::new();
        for (flag, name) in [
            (0x001, "REQUEST"),
            (0x004, "ACK"),
            (0x200, "EXCL"),
            (0x400, "CREATE"),
        ] {
            if flags & flag != 0 {
                names.push(name);
            }
        }
        names.join("|")
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// An `NLMSG_ERROR` framed as the kernel frames an acknowledgment: the
    /// code, then the header of the message it answers.
    fn reply(sequence: u32, code: i32) -> Vec<u8> {
        let mut error = netlink_packet_core::ErrorMessage::default();
        error.code = std::num::NonZeroI32::new(code);
        error.header = vec![0; 16];
        let mut header = netlink_packet_core::NetlinkHeader::default();
        header.sequence_number = sequence;

        let mut message = netlink_packet_core::NetlinkMessage::<
            netlink_packet_netfilter::NetfilterMessage,
        >::new(header, netlink_packet_core::NetlinkPayload::Error(error));
        message.finalize();
        let mut bytes = vec![0; message.buffer_len()];
        message.serialize(&mut bytes);
        bytes
    }

    /// A message the library would not write: any type, any body, and a
    /// length field free to contradict both.
    fn raw(kind: u16, sequence: u32, body: &[u8], length: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(length.to_ne_bytes());
        bytes.extend(kind.to_ne_bytes());
        bytes.extend(0u16.to_ne_bytes());
        bytes.extend(sequence.to_ne_bytes());
        bytes.extend(0u32.to_ne_bytes());
        bytes.extend(body);
        bytes
    }
}
