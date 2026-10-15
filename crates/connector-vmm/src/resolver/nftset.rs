//! The resolver's writes to the `resolved` set, as nfnetlink batches built by
//! hand.
//!
//! One batch shape: `NFNL_MSG_BATCH_BEGIN`, one `NFT_MSG_DELSETELEM` per
//! address being refreshed, one `NFT_MSG_NEWSETELEM` carrying every address
//! with its timeout, `NFNL_MSG_BATCH_END`. nf_tables applies a batch as one
//! transaction, so an address that is deleted and re-added in the same batch
//! is never absent from the set and a connection through it is never
//! interrupted.
//!
//! Encoding the fixed batch shape directly avoids spawning `nft` per answer.
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

use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
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

const NETLINK_NETFILTER: libc::c_int = 12;

const NLM_F_REQUEST: u16 = 0x001;
const NLM_F_ACK: u16 = 0x004;
const NLM_F_EXCL: u16 = 0x200;
const NLM_F_CREATE: u16 = 0x400;
const NLMSG_ERROR: u16 = 2;
const NLMSG_HEADER_LEN: usize = 16;

const NFNL_SUBSYS_NFTABLES: u16 = 10;
const NFNL_MSG_BATCH_BEGIN: u16 = 16;
const NFNL_MSG_BATCH_END: u16 = 17;
const NFT_MSG_NEWSETELEM: u16 = 12;
const NFT_MSG_DELSETELEM: u16 = 14;
const NFPROTO_UNSPEC: u8 = 0;
const NFPROTO_INET: u8 = 1;

const NFTA_SET_ELEM_LIST_TABLE: u16 = 1;
const NFTA_SET_ELEM_LIST_SET: u16 = 2;
const NFTA_SET_ELEM_LIST_ELEMENTS: u16 = 3;
const NFTA_LIST_ELEM: u16 = 1;
const NFTA_SET_ELEM_KEY: u16 = 1;
const NFTA_SET_ELEM_TIMEOUT: u16 = 4;
const NFTA_DATA_VALUE: u16 = 1;
const NLA_F_NESTED: u16 = 0x8000;

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
        // SAFETY: a socket call with constant arguments; the descriptor is
        // taken by OwnedFd below, or the call failed and there is none.
        let raw = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                NETLINK_NETFILTER,
            )
        };
        if raw < 0 {
            return Err(anyhow::anyhow!(std::io::Error::last_os_error())
                .context("opening a netlink socket"));
        }
        // SAFETY: `raw` is a fresh descriptor this call owns.
        let socket = unsafe { OwnedFd::from_raw_fd(raw) };

        // Port 0 and no groups: the kernel, and it assigns our port.
        // SAFETY: sockaddr_nl is plain data for which all-zero is the valid
        // "unbound, addressed to the kernel" value.
        let mut address: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
        address.nl_family = libc::AF_NETLINK as u16;
        // SAFETY: `address` is a correctly sized sockaddr_nl that outlives the
        // call, and the descriptor is open.
        let connected = unsafe {
            libc::connect(
                socket.as_raw_fd(),
                std::ptr::addr_of!(address).cast(),
                std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if connected < 0 {
            return Err(anyhow::anyhow!(std::io::Error::last_os_error())
                .context("connecting the netlink socket to the kernel"));
        }

        Ok(Netlink {
            socket,
            sequence: 1,
        })
    }

    /// Each receive waits only for what is left of the batch's deadline.
    fn set_receive_timeout(&self, remaining: Duration) -> std::io::Result<()> {
        // A zero timeval means "block forever", so never round down to one.
        let remaining = remaining.max(Duration::from_micros(1));
        let timeout = libc::timeval {
            tv_sec: remaining.as_secs() as libc::time_t,
            tv_usec: remaining.subsec_micros() as libc::suseconds_t,
        };
        // SAFETY: `timeout` is a correctly sized timeval that outlives the
        // call, and the descriptor is open.
        let set = unsafe {
            libc::setsockopt(
                self.socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                std::ptr::addr_of!(timeout).cast(),
                std::mem::size_of::<libc::timeval>() as libc::socklen_t,
            )
        };
        if set < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
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

        // SAFETY: the slice outlives the call and its length is its own.
        let sent = unsafe {
            libc::send(
                self.socket.as_raw_fd(),
                batch.as_ptr().cast(),
                batch.len(),
                0,
            )
        };
        if sent < 0 {
            return Err(Failure::Other(
                anyhow::anyhow!(std::io::Error::last_os_error()).context("sending a netlink batch"),
            ));
        }
        self.read_acknowledgments(first, last, deletes.len(), deadline)
    }

    /// Every sequence of the batch must be acknowledged without error, and the
    /// `NFNL_MSG_BATCH_END` acknowledgment must be among them.
    fn read_acknowledgments(
        &self,
        first: u32,
        last: u32,
        deletes: usize,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let mut acknowledged = vec![false; (last - first + 1) as usize];
        let mut buffer = [0u8; 8192];

        while !acknowledged[(last - first) as usize] {
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
            // SAFETY: the buffer outlives the call and its length is its own.
            let read = unsafe {
                libc::recv(
                    self.socket.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    0,
                )
            };
            if read <= 0 {
                let error = std::io::Error::last_os_error();
                return Err(Failure::Other(anyhow::anyhow!(
                    "no acknowledgment for the netlink batch: {error}"
                )));
            }
            let read = read as usize;
            let mut offset = 0;

            while offset + NLMSG_HEADER_LEN <= read {
                let length = u32::from_ne_bytes(buffer[offset..offset + 4].try_into().unwrap());
                let kind = u16::from_ne_bytes(buffer[offset + 4..offset + 6].try_into().unwrap());
                let sequence =
                    u32::from_ne_bytes(buffer[offset + 8..offset + 12].try_into().unwrap());
                let length = length as usize;
                if length < NLMSG_HEADER_LEN || offset + length > read {
                    return Err(Failure::Other(anyhow::anyhow!(
                        "netlink reply of {length} bytes at offset {offset} is malformed"
                    )));
                }

                // Replies to an abandoned batch carry an older sequence.
                if kind == NLMSG_ERROR && (first..=last).contains(&sequence) {
                    let code =
                        i32::from_ne_bytes(buffer[offset + 16..offset + 20].try_into().unwrap());
                    if code != 0 {
                        return Err(classify(sequence, first, deletes, -code));
                    }
                    acknowledged[(sequence - first) as usize] = true;
                }
                offset += length.next_multiple_of(4);
            }
        }

        if let Some(index) = acknowledged.iter().position(|seen| !seen) {
            return Err(Failure::Other(anyhow::anyhow!(
                "the netlink batch committed without acknowledging message {}",
                first + index as u32
            )));
        }
        Ok(())
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
    let mut batch = Vec::new();
    let marker = generation_message(NFPROTO_UNSPEC, NFNL_SUBSYS_NFTABLES);
    let mut sequence = first;

    message(
        NFNL_MSG_BATCH_BEGIN,
        NLM_F_REQUEST | NLM_F_ACK,
        sequence,
        &marker,
        &mut batch,
    );

    for address in deletes {
        sequence += 1;
        let element = element(*address, None);
        let body = set_elements(table, set, &element);
        message(
            (NFNL_SUBSYS_NFTABLES << 8) | NFT_MSG_DELSETELEM,
            NLM_F_REQUEST | NLM_F_ACK,
            sequence,
            &body,
            &mut batch,
        );
    }

    let mut elements = Vec::new();
    for add in adds {
        elements.extend(element(add.address, Some(add.timeout)));
    }
    sequence += 1;
    let body = set_elements(table, set, &elements);
    message(
        (NFNL_SUBSYS_NFTABLES << 8) | NFT_MSG_NEWSETELEM,
        NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL,
        sequence,
        &body,
        &mut batch,
    );

    sequence += 1;
    message(
        NFNL_MSG_BATCH_END,
        NLM_F_REQUEST | NLM_F_ACK,
        sequence,
        &marker,
        &mut batch,
    );
    batch
}

/// `nfgenmsg`: family, version, and a big-endian resource id.
fn generation_message(family: u8, resource: u16) -> Vec<u8> {
    let mut body = vec![family, 0];
    body.extend(resource.to_be_bytes());
    body
}

fn set_elements(table: &str, set: &str, elements: &[u8]) -> Vec<u8> {
    let mut body = generation_message(NFPROTO_INET, 0);
    attribute(NFTA_SET_ELEM_LIST_TABLE, &nul_terminated(table), &mut body);
    attribute(NFTA_SET_ELEM_LIST_SET, &nul_terminated(set), &mut body);
    attribute(
        NFTA_SET_ELEM_LIST_ELEMENTS | NLA_F_NESTED,
        elements,
        &mut body,
    );
    body
}

/// One `NFTA_LIST_ELEM`. A delete carries the key alone; an insert carries the
/// timeout as big-endian milliseconds beside it.
fn element(address: Ipv4Addr, timeout: Option<Duration>) -> Vec<u8> {
    let mut key = Vec::new();
    attribute(NFTA_DATA_VALUE, &address.octets(), &mut key);

    let mut body = Vec::new();
    attribute(NFTA_SET_ELEM_KEY | NLA_F_NESTED, &key, &mut body);

    if let Some(timeout) = timeout {
        let milliseconds = timeout.as_millis().min(u64::MAX as u128) as u64;
        attribute(
            NFTA_SET_ELEM_TIMEOUT,
            &milliseconds.to_be_bytes(),
            &mut body,
        );
    }

    let mut element = Vec::new();
    attribute(NFTA_LIST_ELEM | NLA_F_NESTED, &body, &mut element);
    element
}

fn nul_terminated(value: &str) -> Vec<u8> {
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

/// `nlattr`: a 4-byte header and a payload padded to a 4-byte boundary.
fn attribute(kind: u16, payload: &[u8], out: &mut Vec<u8>) {
    out.extend(((payload.len() + 4) as u16).to_ne_bytes());
    out.extend(kind.to_ne_bytes());
    out.extend(payload);
    out.resize(out.len().next_multiple_of(4), 0);
}

/// `nlmsghdr` and its body, padded to a 4-byte boundary.
fn message(kind: u16, flags: u16, sequence: u32, body: &[u8], out: &mut Vec<u8>) {
    out.extend(((NLMSG_HEADER_LEN + body.len()) as u32).to_ne_bytes());
    out.extend(kind.to_ne_bytes());
    out.extend(flags.to_ne_bytes());
    out.extend(sequence.to_ne_bytes());
    out.extend(0u32.to_ne_bytes());
    out.extend(body);
    out.resize(out.len().next_multiple_of(4), 0);
}

#[cfg(test)]
mod tests {
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

    /// This module names the table and set as strings; the ruleset renders
    /// them. Neither can move without the other.
    #[test]
    fn names_match_the_ruleset() {
        let policy = egress::parse(br#"{"egress":"public"}"#).expect("fixture parses");
        let text = crate::ruleset::render(&policy, &[]).expect("fixture renders");

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

    /// The batch, message by message and attribute by attribute, with the
    /// bytes beside each field.
    fn annotate(batch: &[u8]) -> String {
        let mut rendered = String::new();
        let mut offset = 0;

        while offset + super::NLMSG_HEADER_LEN <= batch.len() {
            let length = u32::from_ne_bytes(batch[offset..offset + 4].try_into().unwrap()) as usize;
            let kind = u16::from_ne_bytes(batch[offset + 4..offset + 6].try_into().unwrap());
            let flags = u16::from_ne_bytes(batch[offset + 6..offset + 8].try_into().unwrap());
            let sequence = u32::from_ne_bytes(batch[offset + 8..offset + 12].try_into().unwrap());

            rendered.push_str(&format!(
                "byte {offset}: nlmsghdr len={length} type={} flags={} seq={sequence} pid=0\n",
                message_name(kind),
                flag_names(flags),
            ));
            let body = &batch[offset + super::NLMSG_HEADER_LEN..offset + length];
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
        let indent = 5 + level * 2;

        while payload.len() >= 4 {
            let length = u16::from_ne_bytes(payload[0..2].try_into().unwrap()) as usize;
            let kind = u16::from_ne_bytes(payload[2..4].try_into().unwrap());
            let nested = kind & super::NLA_F_NESTED != 0;
            let value = &payload[4..length];

            rendered.push_str(&format!(
                "{:indent$}{} len={length}{}\n",
                "",
                attribute_name(kind & !super::NLA_F_NESTED, level),
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
            super::NFNL_MSG_BATCH_BEGIN => "NFNL_MSG_BATCH_BEGIN".to_string(),
            super::NFNL_MSG_BATCH_END => "NFNL_MSG_BATCH_END".to_string(),
            _ => {
                let name = match kind & 0xFF {
                    super::NFT_MSG_NEWSETELEM => "NFT_MSG_NEWSETELEM",
                    super::NFT_MSG_DELSETELEM => "NFT_MSG_DELSETELEM",
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
            (0, super::NFTA_SET_ELEM_LIST_TABLE) => "NFTA_SET_ELEM_LIST_TABLE",
            (0, super::NFTA_SET_ELEM_LIST_SET) => "NFTA_SET_ELEM_LIST_SET",
            (0, super::NFTA_SET_ELEM_LIST_ELEMENTS) => "NFTA_SET_ELEM_LIST_ELEMENTS",
            (1, super::NFTA_LIST_ELEM) => "NFTA_LIST_ELEM",
            (2, super::NFTA_SET_ELEM_KEY) => "NFTA_SET_ELEM_KEY",
            (2, super::NFTA_SET_ELEM_TIMEOUT) => "NFTA_SET_ELEM_TIMEOUT (be64 ms)",
            (3, super::NFTA_DATA_VALUE) => "NFTA_DATA_VALUE",
            _ => "?",
        }
    }

    fn flag_names(flags: u16) -> String {
        let mut names = Vec::new();
        for (flag, name) in [
            (super::NLM_F_REQUEST, "REQUEST"),
            (super::NLM_F_ACK, "ACK"),
            (super::NLM_F_EXCL, "EXCL"),
            (super::NLM_F_CREATE, "CREATE"),
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
}
