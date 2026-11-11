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
//! What the kernel was measured to do, on 7.0.0-1011-gcp with nftables 1.0.9:
//!
//! - An error on any message aborts the whole transaction, including the
//!   messages that individually acknowledged success. An inner `OK` therefore
//!   does not mean the batch landed.
//! - A committed batch acknowledges every sequence it was sent, ending with
//!   the `NFNL_MSG_BATCH_END` acknowledgment. An aborted one never sends that
//!   last acknowledgment. It is the only commit confirmation there is.
//! - Deleting an element that is not present fails with `ENOENT` and takes the
//!   paired insert down with it, which `apply` recovers from below.
//! - Re-adding a live element without `NLM_F_EXCL` silently replaces its
//!   timeout, including shortening it. With `NLM_F_EXCL` it fails with
//!   `EEXIST`, which is why the insert carries that flag: a surviving element
//!   means this module's model of the set is wrong, and that is worth hearing
//!   about rather than absorbing.
//! - An expired element never blocks an insert, whether or not it has been
//!   garbage collected yet.
//! - Each acknowledgment is its own datagram. The kernel queues all of a
//!   batch's acknowledgments before the first receive. On 7.0.0-1014-gcp, a
//!   batch of 259 messages overflowed the default 212992-byte receive buffer,
//!   and the receive failed with `ENOBUFS`.

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
    /// The delete at this index named an element the kernel no longer holds.
    Absent(usize),
    Other(anyhow::Error),
}

impl Netlink {
    pub fn open() -> anyhow::Result<Self> {
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
        // Advance whether or not this transaction succeeds, so a reply to an
        // abandoned batch can never be read as a reply to the next one.
        let last = first + deletes.len() as u32 + 2;
        self.sequence = last + 1;

        if let Err(error) = rustix::net::send(&self.socket, &batch, rustix::net::SendFlags::empty())
        {
            return Err(Failure::Other(
                anyhow::anyhow!(error).context("sending a netlink batch"),
            ));
        }
        self.read_acknowledgments(Acknowledgments::new(first, last, deletes.len()), deadline)
    }

    fn read_acknowledgments(
        &self,
        mut acknowledgments: Acknowledgments,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let mut buffer = [0u8; 8192];

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

/// Tracks every acknowledgment from `first` through the batch-end `last`.
struct Acknowledgments {
    first: u32,
    last: u32,
    deletes: usize,
    seen: Vec<bool>,
}

impl Acknowledgments {
    fn new(first: u32, last: u32, deletes: usize) -> Self {
        Acknowledgments {
            first,
            last,
            deletes,
            seen: vec![false; (last - first + 1) as usize],
        }
    }

    /// Reads every message of one datagram. `Ok(true)` means the batch
    /// committed: the `NFNL_MSG_BATCH_END` acknowledgment has arrived, and so
    /// has a successful one for every other sequence.
    fn receive(&mut self, datagram: &[u8]) -> Result<bool, Failure> {
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
                || !(self.first..=self.last).contains(&sequence)
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
                return Err(classify(sequence, self.first, self.deletes, errno));
            }
            self.seen[(sequence - self.first) as usize] = true;
        }

        if self.seen.last() == Some(&false) {
            return Ok(false);
        }
        if let Some(index) = self.seen.iter().position(|seen| !seen) {
            return Err(Failure::Other(anyhow::anyhow!(
                "the netlink batch committed without acknowledging message {}",
                self.first + index as u32
            )));
        }
        Ok(true)
    }
}

/// An `ENOENT` on a delete means the element expired between the resolver
/// deciding it was still held and the kernel committing. Everything else is a
/// failure the resolver cannot reason its way out of.
fn classify(sequence: u32, first: u32, deletes: usize, errno: i32) -> Failure {
    let index = sequence.saturating_sub(first + 1) as usize;
    let is_delete = sequence > first && index < deletes;

    if is_delete && errno == libc::ENOENT {
        return Failure::Absent(index);
    }
    Failure::Other(anyhow::anyhow!(
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

/// The resolver deletes every element the kernel may still hold, which is one
/// more than it certainly holds. An element that expired in between takes the
/// whole transaction down with `ENOENT`, so that delete is dropped and the
/// batch is sent again. Each attempt drops exactly one, so this runs at most
/// once per delete plus the attempt that commits.
fn retry(
    deletes: &[Ipv4Addr],
    mut attempt: impl FnMut(&[Ipv4Addr]) -> Result<(), Failure>,
) -> anyhow::Result<()> {
    let mut deletes = deletes.to_vec();

    for _ in 0..=deletes.len() {
        match attempt(&deletes) {
            Ok(()) => return Ok(()),
            Err(Failure::Absent(index)) => {
                deletes.remove(index);
            }
            Err(Failure::Other(error)) => return Err(error),
        }
    }
    anyhow::bail!("a netlink batch could not be committed after dropping every stale delete")
}

/// The batch, as bytes. Pure, so the encoding is snapshot-tested without a
/// kernel. Sequences run `first` (the batch marker) through
/// `first + deletes.len() + 2` (the closing marker).
pub fn encode_batch(
    table: &str,
    set: &str,
    deletes: &[Ipv4Addr],
    adds: &[Element],
    first: u32,
) -> Vec<u8> {
    const REQUEST: u16 = netlink_packet_core::NLM_F_REQUEST | netlink_packet_core::NLM_F_ACK;

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
        REQUEST | netlink_packet_core::NLM_F_CREATE | netlink_packet_core::NLM_F_EXCL,
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
                super::Failure::Absent(index) => format!("absent delete {index}"),
                super::Failure::Other(error) => format!("{error:#}"),
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
                Some(index) => Err(super::Failure::Absent(index)),
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

    /// The worst case the loop has to cover: every held element turned out to
    /// have expired, so the batch is resent once per delete before committing
    /// with none.
    #[test]
    fn every_delete_can_be_stale() {
        let held = [address(1), address(2)];
        let mut attempts = 0;

        super::retry(&held, |deletes| {
            attempts += 1;
            match deletes.is_empty() {
                true => Ok(()),
                false => Err(super::Failure::Absent(0)),
            }
        })
        .expect("the batch commits once the last stale delete is dropped");

        assert_eq!(
            attempts, 3,
            "one attempt per delete, plus the one that commits"
        );
    }

    /// A batch of two deletes runs from sequence 7 (the opening marker) to 11
    /// (the closing one). Each case is the datagrams one receive sees.
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
                "one per datagram, as the kernel sends them",
                (7..=11).map(|sequence| reply(sequence, 0)).collect(),
            ),
            ("coalesced", vec![replies(&[7, 8, 9, 10, 11])]),
            (
                "split unevenly and out of order",
                vec![replies(&[9, 7]), replies(&[10, 8, 11])],
            ),
            (
                "inner successes without the closing marker's",
                vec![replies(&[7, 8, 9, 10])],
            ),
            (
                "the closing marker's before another",
                vec![replies(&[7, 8, 9]), replies(&[11])],
            ),
            (
                "replies to abandoned batches, and messages that are not acknowledgments",
                vec![
                    [
                        reply(6, -libc::ENOENT),
                        reply(12, -libc::EPERM),
                        raw(NLMSG_ERROR, 5, &[], 16),
                        raw(NEWSETELEM, 8, &[0xff; 4], 20),
                        raw(NLMSG_DONE, 11, &[], 16),
                    ]
                    .concat(),
                    replies(&[7, 8, 9, 10, 11]),
                ],
            ),
            (
                "ENOENT on delete 0",
                vec![replies(&[7]), reply(8, -libc::ENOENT)],
            ),
            (
                "ENOENT on delete 1",
                vec![replies(&[7, 8]), reply(9, -libc::ENOENT)],
            ),
            ("EEXIST on a delete", vec![reply(8, -libc::EEXIST)]),
            ("ENOENT on the insert", vec![reply(10, -libc::ENOENT)]),
            ("EEXIST on the insert", vec![reply(10, -libc::EEXIST)]),
            ("EPERM on the opening marker", vec![reply(7, -libc::EPERM)]),
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
                    Err(super::Failure::Absent(index)) => (format!("absent delete {index}"), true),
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

    /// Replies shaped as the kernel sent them, through `retry`. Each message
    /// is acknowledged in its own datagram, a failing one with its errno, and
    /// an aborted batch never acknowledges its closing marker. Replies left
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
                first = last + 1;
                rendered.push_str(&format!("attempt {batch}..={last} deletes {deletes:?}\n"));

                let mut codes = vec![0];
                for delete in deletes {
                    codes.push(if held.contains(delete) {
                        0
                    } else {
                        -libc::ENOENT
                    });
                }
                codes.push(insert);
                if codes.iter().all(|code| *code == 0) {
                    codes.push(0);
                }
                queue.extend(
                    (batch..)
                        .zip(codes)
                        .map(|(sequence, code)| reply(sequence, code)),
                );

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
