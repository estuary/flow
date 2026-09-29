//! The guest's only nameserver, and the only way an address gets into the
//! `resolved` set.
//!
//! One thread handles queries sequentially, committing each set update before
//! sending the answer.
//!
//! What a query meets, in order:
//!
//! 1. More than one question, or a question that is not class-IN `A`/`AAAA`:
//!    forwarded byte for byte, authorizing nothing. This deliberately permits
//!    DNS traffic outside the name gate; see the crate README.
//! 2. `AAAA`: `NOERROR` with no answer, never forwarded. No IPv6 reaches the
//!    tap, so an address that resolved would only be something the guest fails
//!    to connect to, slowly. This precedes the name gate because the guest's
//!    own resolver needs the empty answer to move on to its `A` query.
//! 3. `A` for a name that is neither admitted by `allowedNames` nor an
//!    unexpired remembered CNAME target, with `allowAll` off: `REFUSED`,
//!    nothing sent upstream, nothing added to the set.
//! 4. Otherwise forwarded. One answer address inside the baseline condemns the
//!    whole answer: a partial answer would teach the guest which name resolves
//!    inward.
//! 5. Otherwise each `A` and `CNAME` TTL is clamped independently and
//!    rewritten in place, every address is authorized and every target
//!    remembered until at least its clamped TTL, and the set is updated and
//!    acknowledged before the answer is sent.

mod dns;
mod nftset;

use crate::policy::{AllowedName, Policy};
use ipnetwork::Ipv4Network;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

/// A UDP answer is bounded by the datagram, and `recv` truncates silently, so
/// the buffers are the largest a datagram can be rather than the largest
/// answer we expect.
const BUFFER: usize = 65_536;

const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// How long an expired entry is kept after its expiry. The kernel counts an
/// element against the set's size until it garbage collects it, which was
/// measured at under a second past expiry, and an expiry recorded here is
/// already later than the kernel's. Keeping entries this much longer means
/// this side's count is never lower than the kernel's, so the cap below bites
/// before the set can fill.
const RECLAIM: Duration = Duration::from_secs(10);

/// Share the kernel set's cap so exhaustion returns SERVFAIL before an insert
/// can fail fatally with ENFILE. The same cap bounds remembered CNAME targets.
const CAP: usize = crate::ruleset::RESOLVED_SIZE;

pub struct Config {
    listen: SocketAddr,
    upstream: SocketAddr,
    debug: bool,
    allow_all: bool,
    allowed_names: Vec<AllowedName>,
    ttl_floor: u32,
    ttl_cap: u32,
    baseline: Vec<Ipv4Network>,
}

impl Config {
    /// `policy` is already normalized and validated: names are lowercase, and
    /// `ttl_floor` is at least one second, so a clamped TTL is never zero and
    /// never becomes an element that does not expire.
    pub fn new(
        policy: &Policy,
        vmm_subnets: &[Ipv4Network],
        listen: SocketAddr,
        upstream: SocketAddr,
        debug: bool,
    ) -> Self {
        Config {
            listen,
            upstream,
            debug,
            allow_all: policy.allow_all,
            allowed_names: policy.allowed_names.clone(),
            ttl_floor: policy.ttl_floor_secs,
            ttl_cap: policy.ttl_cap_secs,
            baseline: crate::policy::baseline(vmm_subnets),
        }
    }

    fn clamp(&self, ttl: u32) -> u32 {
        ttl.clamp(self.ttl_floor, self.ttl_cap)
    }
}

/// Bind the resolver's sockets and hand the guest's DNS to a thread that is
/// never joined. Returns once it is listening, so every startup failure is the
/// caller's to report before the VM starts.
pub fn start(config: Config) -> anyhow::Result<()> {
    // The ruleset admits DNS only to the tap address. Binding the wildcard
    // would also answer on the uplink, where nothing should reach us.
    if config.listen.ip().is_unspecified() {
        anyhow::bail!(
            "the resolver must bind the guest's nameserver address, not {}",
            config.listen
        );
    }
    let listener = std::net::UdpSocket::bind(config.listen)
        .map_err(|e| anyhow::anyhow!("binding {}: {e}", config.listen))?;

    let writer = nftset::Netlink::open()?;
    let mut resolver = Resolver::new(config, Box::new(writer));

    std::thread::Builder::new()
        .name("resolver".to_string())
        .spawn(move || {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                resolver.serve(&listener)
            })) {
                Ok(Ok(never)) => match never {},
                Ok(Err(error)) => fatal(error),
                Err(_) => fatal(anyhow::anyhow!("the resolver thread panicked")),
            }
        })
        .map_err(|e| anyhow::anyhow!("spawning the resolver thread: {e}"))?;
    Ok(())
}

/// A guest whose nameserver has died cannot be allowed to keep running, so
/// there is no path back from here. `abort` rather than `exit`: the vCPU
/// threads are inside libkrun and never unwind.
fn fatal(error: anyhow::Error) -> ! {
    eprint!("{}", crate::framed(&format!("{error:#}")));
    std::process::abort()
}

pub struct Resolver {
    config: Config,
    writer: Box<dyn nftset::SetWriter>,
    /// Every address this resolver has put in `resolved`, against the expiry it
    /// last promised. Authoritative: the kernel is never asked what it holds.
    addresses: HashMap<Ipv4Addr, Instant>,
    /// CNAME targets seen in allowed answers, against their expiry. A later
    /// `A` query for one of these is allowed, which is what makes a client
    /// that re-queries a target by name work.
    targets: HashMap<String, Instant>,
    scratch: Vec<u8>,
}

/// What the resolver did with one query, and the bytes to send back if any.
pub struct Handled {
    pub decision: Decision,
    pub answer: Option<Vec<u8>>,
}

pub struct Decision {
    pub name: String,
    pub qtype: u16,
    pub outcome: Outcome,
    /// Addresses authorized by this answer, against the TTL written into it.
    pub addresses: Vec<(Ipv4Addr, u32)>,
    pub targets: Vec<(String, u32)>,
    /// The addresses whose elements this answer deleted before re-adding them.
    pub refreshed: Vec<Ipv4Addr>,
}

pub enum Outcome {
    /// Forwarded without gating: more than one question, or not class-IN A.
    Ungated,
    EmptyAaaa,
    RefusedName,
    RefusedPrivate(Ipv4Addr),
    Resolved,
    /// The address or target cap was reached; nothing was authorized.
    Full(&'static str),
    /// Request-local: the guest retries, and authorization is unchanged.
    Dropped(String),
}

/// A question name comes off the wire and an upstream failure carries whatever
/// text the operating system gave it, so both are escaped: one line per
/// decision, and never a line that begins with a space.
impl std::fmt::Display for Decision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let outcome = match &self.outcome {
            Outcome::Ungated => "ungated".to_string(),
            Outcome::EmptyAaaa => "aaaa-empty".to_string(),
            Outcome::RefusedName => "refused-name".to_string(),
            Outcome::RefusedPrivate(address) => format!("refused-private={address}"),
            Outcome::Resolved => "resolved".to_string(),
            Outcome::Full(what) => format!("servfail-full={what}"),
            Outcome::Dropped(reason) => format!("dropped={}", reason.escape_debug()),
        };
        let mut addresses = String::new();
        for (address, ttl) in &self.addresses {
            let _ = write!(addresses, "{}{address}/{ttl}", separator(&addresses));
        }
        let mut targets = String::new();
        for (target, ttl) in &self.targets {
            let _ = write!(
                targets,
                "{}{}/{ttl}",
                separator(&targets),
                target.escape_debug()
            );
        }
        let mut refreshed = String::new();
        for address in &self.refreshed {
            let _ = write!(refreshed, "{}{address}", separator(&refreshed));
        }
        write!(
            f,
            "name={} qtype={} outcome={outcome} addresses=[{addresses}] \
             targets=[{targets}] refreshed=[{refreshed}]",
            self.name.escape_debug(),
            self.qtype,
        )
    }
}

fn separator(accumulated: &str) -> &'static str {
    if accumulated.is_empty() { "" } else { "," }
}

impl Resolver {
    fn new(config: Config, writer: Box<dyn nftset::SetWriter>) -> Self {
        Resolver {
            config,
            writer,
            addresses: HashMap::new(),
            targets: HashMap::new(),
            scratch: Vec::new(),
        }
    }

    /// The loop, which only ends by returning an error that kills the process.
    fn serve(
        &mut self,
        listener: &std::net::UdpSocket,
    ) -> anyhow::Result<std::convert::Infallible> {
        let mut query = vec![0u8; BUFFER];

        loop {
            let (read, client) = match listener.recv_from(&mut query) {
                Ok(received) => received,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(anyhow::anyhow!(error)
                        .context("the resolver's listening socket stopped receiving"));
                }
            };
            let handled = self.handle(&query[..read], Instant::now())?;

            if self.config.debug {
                eprintln!("flow-connector-vmm: {}", handled.decision);
            }
            let Some(answer) = handled.answer else {
                continue;
            };

            // A guest that has gone away is its own problem: it retries, and
            // the authorization it was about to be told about already holds.
            if let Err(error) = listener.send_to(&answer, client)
                && self.config.debug
            {
                eprintln!("flow-connector-vmm: answering {client}: {error}");
            }
        }
    }

    /// One query. `Err` is fatal and only ever comes from the set writer;
    /// every other failure is request-local and shows up as `Outcome::Dropped`.
    pub fn handle(&mut self, query: &[u8], now: Instant) -> anyhow::Result<Handled> {
        self.prune(now);

        let header = match dns::header(query) {
            Ok(header) => header,
            Err(error) => return Ok(dropped(String::new(), 0, &error)),
        };
        if header.qdcount != 1 {
            return Ok(self.forwarded(query, header.id, None, String::new(), 0));
        }
        let question = match dns::first_question(query) {
            Ok(question) => question,
            Err(error) => return Ok(dropped(String::new(), 0, &error)),
        };
        let (name, qtype) = (question.name.clone(), question.qtype);

        if question.qclass != dns::CLASS_IN
            || !matches!(question.qtype, dns::TYPE_A | dns::TYPE_AAAA)
        {
            return Ok(self.forwarded(query, header.id, None, name, qtype));
        }
        if question.qtype == dns::TYPE_AAAA {
            return Ok(Handled {
                decision: decision(name, qtype, Outcome::EmptyAaaa),
                answer: Some(dns::respond(query, &question, dns::RCODE_NOERROR)),
            });
        }
        if !self.allowed(&question.name, now) {
            return Ok(Handled {
                decision: decision(name, qtype, Outcome::RefusedName),
                answer: Some(dns::respond(query, &question, dns::RCODE_REFUSED)),
            });
        }

        // Elapsed work is measured from here, so an expiry is dated from when
        // this answer was actually authorized rather than from when the query
        // arrived. A slow upstream would otherwise backdate it by the whole
        // lookup.
        let started = Instant::now();
        let mut response = match self.forward(query, header.id, Some(&question)) {
            Ok(response) => response,
            Err(reason) => {
                return Ok(Handled {
                    decision: decision(name, qtype, Outcome::Dropped(reason)),
                    answer: None,
                });
            }
        };
        let records = match dns::answer_records(&response) {
            Ok(records) => records,
            Err(error) => return Ok(dropped(name, qtype, &error)),
        };

        // One condemned address condemns the answer, before any TTL is touched
        // and before anything is authorized.
        for record in &records {
            let dns::Kind::Address(address) = record.kind else {
                continue;
            };
            if self.config.baseline.iter().any(|net| net.contains(address)) {
                return Ok(Handled {
                    decision: decision(name, qtype, Outcome::RefusedPrivate(address)),
                    answer: Some(dns::respond(query, &question, dns::RCODE_REFUSED)),
                });
            }
        }

        let mut addresses: Vec<(Ipv4Addr, u32)> = Vec::new();
        let mut targets: Vec<(String, u32)> = Vec::new();
        for record in &records {
            let ttl = self.config.clamp(record.ttl);
            dns::set_ttl(&mut response, record, ttl);

            match &record.kind {
                dns::Kind::Address(address) => retain_longest(&mut addresses, *address, ttl),
                dns::Kind::Target(target) => retain_longest(&mut targets, target.clone(), ttl),
            }
        }

        // `prune` has already dropped everything the kernel can no longer be
        // holding, so what remains is what counts against the cap.
        let new_addresses = addresses
            .iter()
            .filter(|(address, _)| !self.addresses.contains_key(address))
            .count();
        let new_targets = targets
            .iter()
            .filter(|(target, _)| !self.targets.contains_key(target))
            .count();

        for (full, what) in [
            (self.addresses.len() + new_addresses > CAP, "addresses"),
            (self.targets.len() + new_targets > CAP, "targets"),
        ] {
            if full {
                return Ok(Handled {
                    decision: decision(name, qtype, Outcome::Full(what)),
                    answer: Some(dns::respond(query, &question, dns::RCODE_SERVFAIL)),
                });
            }
        }

        // A CNAME-only answer, or one whose records were all of another
        // class, authorizes no address and so asks nothing of the kernel.
        let (deletes, adds) = self.plan(&addresses, now + started.elapsed());
        if !adds.is_empty() {
            self.writer.apply(&deletes, &adds)?;
        }

        // Published only now, and dated from now: until the kernel
        // acknowledged, the promise in this answer was not one the ruleset
        // could keep. The kernel started each element's timer before it
        // acknowledged, so every expiry recorded here is later than the
        // kernel's own. That ordering is what makes the insert in `plan` safe
        // to mark exclusive.
        let authorized = now + started.elapsed();
        for add in &adds {
            self.addresses.insert(add.address, authorized + add.timeout);
        }
        for (target, ttl) in &targets {
            let expiry = authorized + Duration::from_secs(u64::from(*ttl));
            let held = self.targets.entry(target.clone()).or_insert(expiry);
            *held = (*held).max(expiry);
        }

        Ok(Handled {
            decision: Decision {
                name,
                qtype,
                outcome: Outcome::Resolved,
                addresses,
                targets,
                refreshed: deletes,
            },
            answer: Some(response),
        })
    }

    /// What this answer asks of the set. An address already held keeps
    /// whichever is longer, its outstanding time or this record's TTL, so a
    /// shorter answer never shortens a TTL the guest was already given.
    ///
    /// `base` is when this answer was authorized. Every recorded expiry is
    /// dated from after the kernel acknowledged the batch that set it, and the
    /// kernel started that element's timer before acknowledging, so the
    /// kernel's expiry is always the earlier of the two. An address this side
    /// records as expired is therefore certainly gone from the kernel, which
    /// is what lets the insert carry `NLM_F_EXCL`. The other direction - still
    /// recorded here but already dropped by the kernel - is the recoverable
    /// one, and `apply` resends without that delete.
    fn plan(
        &self,
        addresses: &[(Ipv4Addr, u32)],
        base: Instant,
    ) -> (Vec<Ipv4Addr>, Vec<nftset::Element>) {
        let mut deletes = Vec::new();
        let mut adds = Vec::new();

        for (address, ttl) in addresses {
            let outstanding = match self.addresses.get(address) {
                Some(expiry) if *expiry > base => {
                    deletes.push(*address);
                    *expiry - base
                }
                _ => Duration::ZERO,
            };
            adds.push(nftset::Element {
                address: *address,
                timeout: outstanding.max(Duration::from_secs(u64::from(*ttl))),
            });
        }
        (deletes, adds)
    }

    /// Forward a query the resolver does not gate, authorizing nothing.
    fn forwarded(
        &mut self,
        query: &[u8],
        id: u16,
        question: Option<&dns::Question>,
        name: String,
        qtype: u16,
    ) -> Handled {
        match self.forward(query, id, question) {
            Ok(response) => Handled {
                decision: decision(name, qtype, Outcome::Ungated),
                answer: Some(response),
            },
            Err(reason) => Handled {
                decision: decision(name, qtype, Outcome::Dropped(reason)),
                answer: None,
            },
        }
    }

    /// `Err` is a reason to drop this one query. A fresh socket per query,
    /// connected so the kernel discards anything not from the upstream, and an
    /// id and question that must match what was asked.
    fn forward(
        &mut self,
        query: &[u8],
        id: u16,
        question: Option<&dns::Question>,
    ) -> Result<Vec<u8>, String> {
        let upstream = self.config.upstream;

        let socket = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
            .map_err(|e| format!("binding an upstream socket: {e}"))?;
        socket
            .connect(upstream)
            .map_err(|e| format!("connecting to {upstream}: {e}"))?;
        socket
            .set_read_timeout(Some(UPSTREAM_TIMEOUT))
            .map_err(|e| format!("setting the upstream timeout: {e}"))?;
        socket
            .send(query)
            .map_err(|e| format!("sending to {upstream}: {e}"))?;

        self.scratch.resize(BUFFER, 0);
        let read = socket
            .recv(&mut self.scratch)
            .map_err(|e| format!("no answer from {upstream}: {e}"))?;
        let response = self.scratch[..read].to_vec();

        let header = dns::header(&response).map_err(|e| format!("upstream answer: {e:#}"))?;
        if header.flags & dns::FLAG_RESPONSE == 0 {
            return Err("upstream sent a query, not a response".to_string());
        }
        if header.id != id {
            return Err(format!(
                "upstream answered id {:#06x}, not {id:#06x}",
                header.id
            ));
        }
        if let Some(question) = question {
            let answered = dns::first_question(&response)
                .map_err(|e| format!("upstream answer's question: {e:#}"))?;

            if answered.name != question.name
                || answered.qtype != question.qtype
                || answered.qclass != question.qclass
            {
                return Err(format!(
                    "upstream answered {:?}/{}, not {:?}/{}",
                    answered.name, answered.qtype, question.name, question.qtype
                ));
            }
        }
        Ok(response)
    }

    fn allowed(&self, name: &str, now: Instant) -> bool {
        if self.config.allow_all {
            return true;
        }
        if self
            .config
            .allowed_names
            .iter()
            .any(|allowed| admits(allowed, name))
        {
            return true;
        }
        self.targets.get(name).is_some_and(|expiry| *expiry > now)
    }

    /// Drop what the kernel can no longer be holding. Entries live past their
    /// expiry on purpose: see `RECLAIM`.
    fn prune(&mut self, now: Instant) {
        self.addresses.retain(|_, expiry| *expiry + RECLAIM > now);
        self.targets.retain(|_, expiry| *expiry + RECLAIM > now);
    }
}

/// Escaped dots within DNS labels cannot masquerade as name boundaries.
fn admits(allowed: &AllowedName, name: &str) -> bool {
    match allowed {
        AllowedName::Exact(exact) => name == exact,
        AllowedName::Subdomains(base) => name
            .strip_suffix(base.as_str())
            .is_some_and(|below| below.ends_with('.')),
    }
}

fn retain_longest<K: PartialEq>(seen: &mut Vec<(K, u32)>, key: K, ttl: u32) {
    match seen.iter_mut().find(|(held, _)| *held == key) {
        Some((_, held)) => *held = (*held).max(ttl),
        None => seen.push((key, ttl)),
    }
}

fn decision(name: String, qtype: u16, outcome: Outcome) -> Decision {
    Decision {
        name,
        qtype,
        outcome,
        addresses: Vec::new(),
        targets: Vec::new(),
        refreshed: Vec::new(),
    }
}

fn dropped(name: String, qtype: u16, error: &anyhow::Error) -> Handled {
    Handled {
        decision: decision(name, qtype, Outcome::Dropped(format!("{error:#}"))),
        answer: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Decision, Handled, Outcome, Resolver, dns, nftset};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    const POLICY: &str = r#"{
        "egress": "public",
        "allowedNames": [
            "pypi.org",
            "*.mirror.acmeco.example",
            "artifacts.acmeco.example",
            "metadata.acmeco.example",
            "cdnonly.acmeco.example",
            "dup.acmeco.example",
            "long.acmeco.example",
            "short.acmeco.example",
            "chaos.acmeco.example"
        ],
        "ttlFloorSecs": 90,
        "ttlCapSecs": 3600
    }"#;

    /// A type the resolver does not gate, so the query is forwarded whole.
    const TYPE_MX: u16 = 15;

    fn public(last: u8) -> Ipv4Addr {
        Ipv4Addr::new(93, 184, 216, last)
    }

    #[derive(Clone)]
    enum Rr {
        A(&'static str, Ipv4Addr, u32),
        Cname(&'static str, &'static str, u32),
        /// An A record in a class other than IN. Nothing may authorize it.
        Chaos(&'static str, Ipv4Addr, u32),
    }

    fn encode_name(name: &str, out: &mut Vec<u8>) {
        for label in name.split('.') {
            out.push(label.len() as u8);
            out.extend(label.as_bytes());
        }
        out.push(0);
    }

    fn query(id: u16, name: &str, qtype: u16) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend(id.to_be_bytes());
        message.extend(0x0100u16.to_be_bytes()); // recursion desired
        message.extend(1u16.to_be_bytes());
        message.extend([0u8; 6]);
        encode_name(name, &mut message);
        message.extend(qtype.to_be_bytes());
        message.extend(dns::CLASS_IN.to_be_bytes());
        message
    }

    /// A query for `name` whose first label holds a literal dot where `name`
    /// has its first hyphen. No hostname can carry one, and a name flattened
    /// without escaping would read it as a label boundary.
    fn dotted_query(id: u16, name: &str) -> Vec<u8> {
        let mut message = query(id, name, dns::TYPE_A);
        let hyphen = name.find('-').expect("a hyphen to replace");
        assert!(hyphen < name.find('.').expect("more than one label"));
        message[12 + 1 + hyphen] = b'.';
        message
    }

    fn two_questions(id: u16, first: &str, second: &str) -> Vec<u8> {
        let mut message = query(id, first, dns::TYPE_A);
        message[4..6].copy_from_slice(&2u16.to_be_bytes());
        encode_name(second, &mut message);
        message.extend(dns::TYPE_A.to_be_bytes());
        message.extend(dns::CLASS_IN.to_be_bytes());
        message
    }

    fn respond(question: &[u8], records: &[Rr]) -> Vec<u8> {
        let mut message = question.to_vec();
        message[2..4].copy_from_slice(&0x8180u16.to_be_bytes()); // response, RD, RA
        message[6..8].copy_from_slice(&(records.len() as u16).to_be_bytes());

        for record in records {
            match record {
                Rr::A(owner, address, ttl) | Rr::Chaos(owner, address, ttl) => {
                    let class = match record {
                        Rr::Chaos(..) => 3u16,
                        _ => dns::CLASS_IN,
                    };
                    encode_name(owner, &mut message);
                    message.extend(dns::TYPE_A.to_be_bytes());
                    message.extend(class.to_be_bytes());
                    message.extend(ttl.to_be_bytes());
                    message.extend(4u16.to_be_bytes());
                    message.extend(address.octets());
                }
                Rr::Cname(owner, target, ttl) => {
                    encode_name(owner, &mut message);
                    message.extend(dns::TYPE_CNAME.to_be_bytes());
                    message.extend(dns::CLASS_IN.to_be_bytes());
                    message.extend(ttl.to_be_bytes());
                    let mut rdata = Vec::new();
                    encode_name(target, &mut rdata);
                    message.extend((rdata.len() as u16).to_be_bytes());
                    message.extend(rdata);
                }
            }
        }
        message
    }

    /// A real UDP nameserver on loopback, answering from a script keyed by
    /// question name and type. An unscripted question gets silence.
    fn upstream(script: Vec<((&'static str, u16), Vec<Rr>)>) -> SocketAddr {
        upstream_taking(script, Duration::ZERO)
    }

    /// The same, but every answer takes `delay` to come back.
    fn upstream_taking(script: Vec<((&'static str, u16), Vec<Rr>)>, delay: Duration) -> SocketAddr {
        let socket =
            std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("binding a fake upstream");
        let address = socket.local_addr().expect("the fake upstream's address");

        std::thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            loop {
                let Ok((read, peer)) = socket.recv_from(&mut buffer) else {
                    return;
                };
                let received = &buffer[..read];
                let Ok(question) = dns::first_question(received) else {
                    continue;
                };
                let Some((_, records)) = script
                    .iter()
                    .find(|((name, qtype), _)| *name == question.name && *qtype == question.qtype)
                else {
                    continue;
                };
                std::thread::sleep(delay);
                let _ = socket.send_to(&respond(received, records), peer);
            }
        });
        address
    }

    type Call = (Vec<Ipv4Addr>, Vec<nftset::Element>);

    #[derive(Clone, Default)]
    struct Recorder {
        calls: Arc<Mutex<Vec<Call>>>,
        failing: Arc<Mutex<bool>>,
    }

    impl Recorder {
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().expect("recorder").clone()
        }
    }

    impl nftset::SetWriter for Recorder {
        fn apply(&mut self, deletes: &[Ipv4Addr], adds: &[nftset::Element]) -> anyhow::Result<()> {
            self.calls
                .lock()
                .expect("recorder")
                .push((deletes.to_vec(), adds.to_vec()));

            if *self.failing.lock().expect("recorder") {
                anyhow::bail!("injected netlink failure");
            }
            Ok(())
        }
    }

    fn resolver(upstream: SocketAddr, writer: Recorder) -> Resolver {
        resolver_under(POLICY, upstream, writer)
    }

    fn resolver_under(policy: &str, upstream: SocketAddr, writer: Recorder) -> Resolver {
        let policy = crate::policy::parse(policy.as_bytes()).expect("fixture parses");
        let config = Config::new(
            &policy,
            &[],
            SocketAddr::from((Ipv4Addr::LOCALHOST, 53)),
            upstream,
            false,
        );
        Resolver::new(config, Box::new(writer))
    }

    fn script() -> Vec<((&'static str, u16), Vec<Rr>)> {
        vec![
            (
                ("pypi.org", dns::TYPE_A),
                vec![Rr::A("pypi.org", public(10), 60)],
            ),
            (
                ("files.pypi.org", dns::TYPE_A),
                vec![Rr::A("files.pypi.org", public(11), 300)],
            ),
            (
                ("eu.mirror.acmeco.example", dns::TYPE_A),
                vec![Rr::A("eu.mirror.acmeco.example", public(80), 300)],
            ),
            (
                ("a.eu.mirror.acmeco.example", dns::TYPE_A),
                vec![Rr::A("a.eu.mirror.acmeco.example", public(81), 300)],
            ),
            (
                ("mirror.acmeco.example", dns::TYPE_A),
                vec![Rr::A("mirror.acmeco.example", public(82), 300)],
            ),
            (
                ("x.assets.acmeco.example", dns::TYPE_A),
                vec![Rr::A("x.assets.acmeco.example", public(43), 300)],
            ),
            (
                ("long.acmeco.example", dns::TYPE_A),
                vec![Rr::A("long.acmeco.example", public(50), 86_400)],
            ),
            (
                ("short.acmeco.example", dns::TYPE_A),
                vec![Rr::A("short.acmeco.example", public(50), 90)],
            ),
            (
                ("metadata.acmeco.example", dns::TYPE_A),
                vec![Rr::A(
                    "metadata.acmeco.example",
                    Ipv4Addr::new(169, 254, 169, 254),
                    300,
                )],
            ),
            (
                ("dup.acmeco.example", dns::TYPE_A),
                vec![
                    Rr::A("dup.acmeco.example", public(60), 100),
                    Rr::A("dup.acmeco.example", public(60), 500),
                ],
            ),
            (
                ("chaos.acmeco.example", dns::TYPE_A),
                vec![Rr::Chaos("chaos.acmeco.example", public(70), 300)],
            ),
            (
                ("artifacts.acmeco.example", dns::TYPE_A),
                vec![
                    Rr::Cname("artifacts.acmeco.example", "assets.acmeco.example", 30),
                    Rr::A("assets.acmeco.example", public(40), 200),
                ],
            ),
            (
                ("assets.acmeco.example", dns::TYPE_A),
                vec![Rr::A("assets.acmeco.example", public(41), 200)],
            ),
            (
                ("cdnonly.acmeco.example", dns::TYPE_A),
                vec![Rr::Cname(
                    "cdnonly.acmeco.example",
                    "edge.acmeco.example",
                    120,
                )],
            ),
            (
                ("edge.acmeco.example", dns::TYPE_A),
                vec![Rr::A("edge.acmeco.example", public(42), 600)],
            ),
            (
                ("pypi.org", TYPE_MX),
                vec![Rr::A("pypi.org", public(10), 300)],
            ),
        ]
    }

    #[test]
    fn decisions() {
        let recorder = Recorder::default();
        let mut resolver = resolver(upstream(script()), recorder.clone());
        let start = Instant::now();

        let steps: Vec<(&str, u64, Vec<u8>)> = vec![
            (
                "allowed name, TTL below the floor",
                0,
                query(1, "pypi.org", dns::TYPE_A),
            ),
            (
                "the same name again: delete and re-add",
                1,
                query(2, "pypi.org", dns::TYPE_A),
            ),
            (
                "the question in mixed case",
                2,
                query(3, "PyPI.ORG", dns::TYPE_A),
            ),
            (
                "a label below an exact name",
                3,
                query(4, "files.pypi.org", dns::TYPE_A),
            ),
            (
                "a name that only looks like one",
                4,
                query(5, "notpypi.org", dns::TYPE_A),
            ),
            (
                "an exact name with labels after it",
                5,
                query(6, "pypi.org.evil.example", dns::TYPE_A),
            ),
            (
                "a label below a wildcard",
                6,
                query(7, "eu.mirror.acmeco.example", dns::TYPE_A),
            ),
            (
                "labels nested below a wildcard",
                7,
                query(8, "a.eu.mirror.acmeco.example", dns::TYPE_A),
            ),
            (
                "a wildcard's own base",
                8,
                query(9, "mirror.acmeco.example", dns::TYPE_A),
            ),
            (
                "a name that only ends like a wildcard's base",
                9,
                query(10, "notmirror.acmeco.example", dns::TYPE_A),
            ),
            (
                "a wildcard's subdomain with labels after it",
                10,
                query(11, "eu.mirror.acmeco.example.evil.example", dns::TYPE_A),
            ),
            (
                "a dot inside a label, posing as an exact name",
                11,
                dotted_query(12, "artifacts-acmeco.example"),
            ),
            (
                "a dot inside a label, posing as a wildcard's subdomain",
                12,
                dotted_query(13, "eu-mirror.acmeco.example"),
            ),
            (
                "a name nobody listed",
                13,
                query(14, "evil.example", dns::TYPE_A),
            ),
            (
                "AAAA for an allowed name",
                14,
                query(15, "pypi.org", dns::TYPE_AAAA),
            ),
            (
                "AAAA for a name nobody listed",
                15,
                query(16, "evil.example", dns::TYPE_AAAA),
            ),
            (
                "a type the resolver does not gate",
                16,
                query(17, "pypi.org", TYPE_MX),
            ),
            (
                "two questions in one query",
                17,
                two_questions(18, "pypi.org", "evil.example"),
            ),
            (
                "an answer pointing inward",
                18,
                query(19, "metadata.acmeco.example", dns::TYPE_A),
            ),
            (
                "TTL above the cap",
                19,
                query(20, "long.acmeco.example", dns::TYPE_A),
            ),
            (
                "the same address, shorter TTL",
                20,
                query(21, "short.acmeco.example", dns::TYPE_A),
            ),
            (
                "the same address twice in one answer",
                21,
                query(22, "dup.acmeco.example", dns::TYPE_A),
            ),
            (
                "an A record in another class",
                22,
                query(23, "chaos.acmeco.example", dns::TYPE_A),
            ),
            (
                "a CNAME chain",
                23,
                query(24, "artifacts.acmeco.example", dns::TYPE_A),
            ),
            (
                "the remembered target, queried directly",
                24,
                query(25, "assets.acmeco.example", dns::TYPE_A),
            ),
            (
                "a label below the remembered target",
                25,
                query(26, "x.assets.acmeco.example", dns::TYPE_A),
            ),
            (
                "a CNAME with no address behind it",
                26,
                query(27, "cdnonly.acmeco.example", dns::TYPE_A),
            ),
            (
                "the target of that CNAME",
                27,
                query(28, "edge.acmeco.example", dns::TYPE_A),
            ),
            (
                "the remembered target once it has expired",
                400,
                query(29, "assets.acmeco.example", dns::TYPE_A),
            ),
        ];

        let mut log = String::new();
        let mut seen = 0;

        for (label, offset, message) in steps {
            let now = start + Duration::from_secs(offset);
            let handled = resolver
                .handle(&message, now)
                .expect("no step of this run fails fatally");

            log.push_str(&format!("# {label} (t+{offset}s)\n"));
            log.push_str(&format!("{}\n", handled.decision));
            log.push_str(&format!("{}\n", summarize(&handled)));

            let calls = recorder.calls();
            for (deletes, adds) in &calls[seen..] {
                log.push_str(&format!("set: {}\n", render_call(deletes, adds)));
            }
            seen = calls.len();
            log.push('\n');
        }
        insta::assert_snapshot!(log);
    }

    fn summarize(handled: &Handled) -> String {
        let Some(answer) = &handled.answer else {
            return "answer: none".to_string();
        };
        let header = dns::header(answer).expect("the resolver answers with a parsable message");
        format!(
            "answer: rcode={} ancount={} bytes={}",
            header.flags & 0x000F,
            header.ancount,
            answer.len(),
        )
    }

    fn render_call(deletes: &[Ipv4Addr], adds: &[nftset::Element]) -> String {
        let deletes: Vec<String> = deletes.iter().map(Ipv4Addr::to_string).collect();
        let adds: Vec<String> = adds
            .iter()
            .map(|add| {
                format!(
                    "{} timeout {}s",
                    add.address,
                    add.timeout.as_secs_f64().round()
                )
            })
            .collect();
        format!("delete [{}] add [{}]", deletes.join(", "), adds.join(", "))
    }

    #[test]
    fn ttls_are_rewritten_in_the_answer() {
        let recorder = Recorder::default();
        let mut resolver = resolver(upstream(script()), recorder);
        let now = Instant::now();

        for (name, expected) in [
            ("pypi.org", vec![90]),                      // 60 upstream, floor 90
            ("long.acmeco.example", vec![3_600]),        // 86400 upstream, cap 3600
            ("artifacts.acmeco.example", vec![90, 200]), // CNAME 30 -> 90, A 200 kept
        ] {
            let handled = resolver
                .handle(&query(1, name, dns::TYPE_A), now)
                .expect("the answer is accepted");
            let answer = handled.answer.expect("an answer is returned");

            let ttls: Vec<u32> = dns::answer_records(&answer)
                .expect("the rewritten answer still parses")
                .iter()
                .map(|record| record.ttl)
                .collect();
            assert_eq!(ttls, expected, "TTLs of {name}");
        }
    }

    #[test]
    fn a_shorter_answer_does_not_shorten_an_outstanding_ttl() {
        let recorder = Recorder::default();
        let mut resolver = resolver(upstream(script()), recorder.clone());
        let start = Instant::now();

        resolver
            .handle(&query(1, "long.acmeco.example", dns::TYPE_A), start)
            .expect("the long answer is accepted");
        resolver
            .handle(
                &query(2, "short.acmeco.example", dns::TYPE_A),
                start + Duration::from_secs(100),
            )
            .expect("the short answer is accepted");

        let calls = recorder.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].1[0].timeout, Duration::from_secs(3_600));
        // 3600 promised at t0 still has about 3500 to run at t0 + 100, which
        // beats the 90 this answer asks for. Not exact: the remainder carries
        // the real time the first answer spent being authorized.
        assert_eq!(
            calls[1].0,
            vec![public(50)],
            "the held element is refreshed"
        );
        let refreshed = calls[1].1[0].timeout;
        assert!(
            refreshed > Duration::from_secs(3_499) && refreshed < Duration::from_secs(3_501),
            "expected about 3500s, got {refreshed:?}"
        );
    }

    #[test]
    fn a_failed_set_update_yields_no_answer() {
        let recorder = Recorder::default();
        *recorder.failing.lock().expect("recorder") = true;
        let mut resolver = resolver(upstream(script()), recorder.clone());

        let Err(error) = resolver.handle(&query(1, "pypi.org", dns::TYPE_A), Instant::now()) else {
            panic!("a failed set update must be fatal, and must not answer");
        };
        assert!(error.to_string().contains("injected"), "{error:#}");
        assert_eq!(recorder.calls().len(), 1, "the set update was attempted");
    }

    /// The caps are the resolver's own, so they are exercised by filling the
    /// maps rather than by resolving a thousand names.
    #[test]
    fn the_caps_answer_servfail() {
        let recorder = Recorder::default();
        let mut resolver = resolver(upstream(script()), recorder.clone());
        let now = Instant::now();
        let expiry = now + Duration::from_secs(600);

        for index in 0..super::CAP {
            resolver
                .addresses
                .insert(Ipv4Addr::from(index as u32), expiry);
        }
        let handled = resolver
            .handle(&query(1, "pypi.org", dns::TYPE_A), now)
            .expect("a full set is not fatal");

        assert!(matches!(
            handled.decision.outcome,
            Outcome::Full("addresses")
        ));
        assert_eq!(rcode(&handled), dns::RCODE_SERVFAIL);
        assert!(
            recorder.calls().is_empty(),
            "nothing was written to the set"
        );

        // An address already held still resolves: it adds nothing new.
        resolver.addresses.insert(public(10), expiry);
        resolver.addresses.remove(&Ipv4Addr::from(0u32));
        let handled = resolver
            .handle(&query(2, "pypi.org", dns::TYPE_A), now)
            .expect("a held address is not new");
        assert!(matches!(handled.decision.outcome, Outcome::Resolved));
    }

    /// An upstream that answers with an ICMP rejection rather than a message
    /// is a request-local drop: the guest retries.
    #[test]
    fn an_unreachable_upstream_drops_the_query() {
        let closed = {
            let socket = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind");
            socket.local_addr().expect("address")
        };
        let recorder = Recorder::default();
        let mut resolver = resolver(closed, recorder.clone());

        let handled = resolver
            .handle(&query(1, "pypi.org", dns::TYPE_A), Instant::now())
            .expect("an unreachable upstream is not fatal");

        assert!(handled.answer.is_none());
        assert!(matches!(handled.decision.outcome, Outcome::Dropped(_)));
        assert!(recorder.calls().is_empty());
    }

    /// An answer carrying somebody else's transaction id is not acted on,
    /// which is the check that a guest guessing a source port still has to
    /// beat the id as well.
    #[test]
    fn an_answer_with_the_wrong_id_is_dropped() {
        let recorder = Recorder::default();
        let mut resolver = resolver(forging_upstream(), recorder.clone());

        let handled = resolver
            .handle(&query(1, "pypi.org", dns::TYPE_A), Instant::now())
            .expect("a forged id is not fatal");

        assert!(handled.answer.is_none());
        assert!(
            matches!(&handled.decision.outcome, Outcome::Dropped(reason) if reason.contains("id")),
            "unexpected outcome"
        );
        assert!(recorder.calls().is_empty(), "nothing was authorized");
    }

    /// An upstream that answers the right question under the wrong id.
    fn forging_upstream() -> SocketAddr {
        let socket =
            std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("binding a fake upstream");
        let address = socket.local_addr().expect("the fake upstream's address");

        std::thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            loop {
                let Ok((read, peer)) = socket.recv_from(&mut buffer) else {
                    return;
                };
                let mut answer = respond(
                    &buffer[..read],
                    &[Rr::A("pypi.org", Ipv4Addr::new(10, 0, 0, 1), 300)],
                );
                answer[0..2].copy_from_slice(&0xBEEFu16.to_be_bytes());
                let _ = socket.send_to(&answer, peer);
            }
        });
        address
    }

    #[test]
    fn a_malformed_query_is_dropped() {
        let recorder = Recorder::default();
        let mut resolver = resolver(upstream(script()), recorder.clone());

        // The header claims a question that the message does not carry.
        let mut message = query(1, "pypi.org", dns::TYPE_A);
        message.truncate(message.len() - 3);
        let handled = resolver
            .handle(&message, Instant::now())
            .expect("a malformed query is not fatal");

        assert!(handled.answer.is_none());
        assert!(matches!(handled.decision.outcome, Outcome::Dropped(_)));
        assert!(recorder.calls().is_empty());
    }

    #[test]
    fn start_refuses_the_wildcard() {
        let policy = crate::policy::parse(POLICY.as_bytes()).expect("fixture parses");
        let config = Config::new(
            &policy,
            &[],
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 53)),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 53)),
            false,
        );
        let error = super::start(config).expect_err("the wildcard is refused");

        assert!(
            error
                .to_string()
                .contains("must bind the guest's nameserver"),
            "{error:#}"
        );
        for line in crate::framed(&format!("{error:#}")).lines() {
            assert!(!line.starts_with(' '), "framed error line {line:?}");
        }
    }

    /// An expiry must be dated from when the answer was authorized, not from
    /// when the query arrived. A lookup slower than the TTL would otherwise
    /// hand the guest a CNAME target this side already considers expired.
    #[test]
    fn a_slow_lookup_does_not_backdate_authorization() {
        const DELAY: Duration = Duration::from_millis(1_100);
        const POLICY: &str = r#"{
            "egress": "public",
            "allowedNames": ["cdnonly.acmeco.example"],
            "ttlFloorSecs": 1,
            "ttlCapSecs": 3600
        }"#;
        let script = vec![
            (
                ("cdnonly.acmeco.example", dns::TYPE_A),
                vec![Rr::Cname(
                    "cdnonly.acmeco.example",
                    "edge.acmeco.example",
                    1,
                )],
            ),
            (
                ("edge.acmeco.example", dns::TYPE_A),
                vec![Rr::A("edge.acmeco.example", public(42), 1)],
            ),
        ];
        let recorder = Recorder::default();
        let mut resolver = resolver_under(POLICY, upstream_taking(script, DELAY), recorder.clone());

        let arrived = Instant::now();
        let handled = resolver
            .handle(&query(1, "cdnonly.acmeco.example", dns::TYPE_A), arrived)
            .expect("the CNAME answer is accepted");
        assert!(matches!(handled.decision.outcome, Outcome::Resolved));

        // The 1s TTL is measured from the answer, which arrived after DELAY.
        let remembered = resolver.targets["edge.acmeco.example"];
        assert!(
            remembered >= arrived + DELAY + Duration::from_secs(1),
            "the target was remembered from before the lookup finished"
        );

        // What the guest does next: follow the CNAME it was just given.
        let handled = resolver
            .handle(
                &query(2, "edge.acmeco.example", dns::TYPE_A),
                Instant::now(),
            )
            .expect("the follow-up is accepted");
        assert!(
            matches!(handled.decision.outcome, Outcome::Resolved),
            "the target the guest was just handed was already expired"
        );

        // And the address expiry carries the lookup too, so it is never
        // earlier than the kernel's.
        let expiry = resolver.addresses[&public(42)];
        assert!(expiry >= arrived + DELAY + Duration::from_secs(1));
    }

    /// Only an address that may still be in the kernel is deleted, and a held
    /// address keeps whichever is longer of its remainder and the new TTL.
    #[test]
    fn only_addresses_that_may_still_be_held_are_deleted() {
        let recorder = Recorder::default();
        let mut resolver = resolver(upstream(script()), recorder);
        let start = Instant::now();
        let base = start + Duration::from_secs(10);

        // Live at `base`, with 30s outstanding.
        resolver
            .addresses
            .insert(public(1), base + Duration::from_secs(30));
        // Recorded as expired before `base`, so the kernel dropped it earlier
        // still and the insert may be exclusive.
        resolver
            .addresses
            .insert(public(2), start + Duration::from_secs(5));

        let (deletes, adds) =
            resolver.plan(&[(public(1), 10), (public(2), 90), (public(3), 90)], base);

        assert_eq!(
            deletes,
            vec![public(1)],
            "only the address that may be held"
        );
        assert_eq!(
            adds.iter().map(|add| add.timeout).collect::<Vec<_>>(),
            vec![
                Duration::from_secs(30), // its 30s remainder beats this 10s TTL
                Duration::from_secs(90),
                Duration::from_secs(90),
            ]
        );
    }

    /// A decision is one line, whatever the guest put in the question. A name
    /// arrives off the wire, so a label holding a newline would otherwise
    /// split the debug log and could produce a line beginning with a space,
    /// which is the launcher's readiness signal.
    #[test]
    fn a_decision_never_spans_lines() {
        let decision = Decision {
            name: "a\nartifacts.acmeco.example".to_string(),
            qtype: dns::TYPE_A,
            outcome: Outcome::Dropped("upstream answered\n a different question".to_string()),
            addresses: vec![(public(40), 200)],
            targets: vec![("as\rsets.acmeco.example".to_string(), 90)],
            refreshed: vec![public(40)],
        };
        let rendered = decision.to_string();

        assert!(!rendered.contains('\n'), "{rendered:?}");
        assert!(!rendered.contains('\r'), "{rendered:?}");
        for line in crate::framed(&rendered).lines() {
            assert!(!line.starts_with(' '), "framed line {line:?}");
        }
        insta::assert_snapshot!(rendered);
    }

    fn rcode(handled: &Handled) -> u16 {
        let answer = handled.answer.as_ref().expect("an answer is returned");
        dns::header(answer).expect("parsable").flags & 0x000F
    }
}
