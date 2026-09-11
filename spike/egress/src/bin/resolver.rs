//! `flow-sandbox-resolver`: the guest's only nameserver, and the only way an
//! address gets into the `resolved` set.
//!
//! One thread, one query at a time. A spike resolver in front of a single
//! connector is not a hot path, and the sequencing makes "the element is in
//! the set before the answer leaves" true by construction rather than by
//! argument.

use flow_sandbox_egress::cidr::Cidr;
use flow_sandbox_egress::{dns, ifaddrs, policy};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const USAGE: &str = "usage: flow-sandbox-resolver --policy PATH --listen A.B.C.D:PORT \
     --upstream A.B.C.D:PORT [--debug]";

/// Bounded because a UDP answer is bounded: 4 KiB covers any EDNS0 payload we
/// advertise upstream, and a truncated answer falls back to TCP, which the
/// ruleset does not carry.
const BUFFER: usize = 4096;
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(5);

struct Args {
    policy: PathBuf,
    listen: SocketAddr,
    upstream: SocketAddr,
    debug: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("flow-sandbox-resolver: {error:#}");
        std::process::exit(2);
    }
}

fn run() -> anyhow::Result<()> {
    let args = parse(std::env::args().skip(1).collect()).map_err(|e| anyhow::anyhow!("{e}"))?;
    let policy = policy::load(&args.policy)?;

    if policy.egress != policy::Mode::Public {
        anyhow::bail!("egress is not `public`; no resolver runs for this policy");
    }
    let denied = policy::denied_prefixes(&ifaddrs::helper_subnets()?)?;

    let socket = UdpSocket::bind(args.listen)
        .map_err(|e| anyhow::anyhow!("binding {}: {e}", args.listen))?;
    eprintln!(
        "flow-sandbox-resolver: listening on {}, upstream {}",
        args.listen, args.upstream
    );

    let mut query = [0u8; BUFFER];
    loop {
        let (length, client) = match socket.recv_from(&mut query) {
            Ok(received) => received,
            Err(error) => {
                eprintln!("flow-sandbox-resolver: recv_from: {error}");
                continue;
            }
        };
        let answer = match answer(&query[..length], &args, &policy, &denied) {
            Ok(answer) => answer,
            Err(error) => {
                eprintln!("flow-sandbox-resolver: {error:#}");
                continue;
            }
        };
        if let Some(answer) = answer {
            if let Err(error) = socket.send_to(&answer, client) {
                eprintln!("flow-sandbox-resolver: send_to {client}: {error}");
            }
        }
    }
}

/// `Ok(None)` means the query is dropped on the floor, which is what upstream
/// silence looks like to the guest: its own resolver retries.
fn answer(
    query: &[u8],
    args: &Args,
    policy: &policy::Policy,
    denied: &[Cidr],
) -> anyhow::Result<Option<Vec<u8>>> {
    let header = dns::header(query)?;
    if header.qdcount != 1 {
        return Ok(forward(query, args.upstream)?);
    }
    let question = dns::first_question(query)?;

    // No IPv6 reaches the tap, so an AAAA that resolves is an address the
    // guest can only fail to connect to, slowly. NOERROR with no answer is
    // what getaddrinfo needs to move on to the A query.
    if question.qtype == dns::TYPE_AAAA {
        debug(args, &question, "aaaa-empty", &[], 0, 0);
        return Ok(Some(dns::respond_empty(query, &question, 0)));
    }

    let Some(mut response) = forward(query, args.upstream)? else {
        return Ok(None);
    };
    let response_question = dns::first_question(&response)?;
    let records = dns::answer_a_records(&response, &response_question)?;
    if records.is_empty() {
        debug(args, &question, "forwarded", &[], 0, 0);
        return Ok(Some(response));
    }

    // One denied address condemns the whole answer: the guest would otherwise
    // retry the names it was given until it found the one that resolves
    // inward, and a partial answer teaches it which one that is.
    if let Some(hit) = records
        .iter()
        .find(|record| denied.iter().any(|prefix| prefix.contains(record.address)))
    {
        let address = hit.address;
        debug(args, &question, "refused", &[address], 0, 0);
        return Ok(Some(dns::respond_empty(
            query,
            &question,
            dns::RCODE_REFUSED,
        )));
    }

    let mut elements: Vec<(Ipv4Addr, u32)> = Vec::new();
    for record in &records {
        let ttl = policy.clamp_ttl(record.ttl);
        dns::set_ttl(&mut response, record, ttl);

        match elements
            .iter_mut()
            .find(|(address, _)| *address == record.address)
        {
            Some((_, existing)) => *existing = (*existing).max(ttl),
            None => elements.push((record.address, ttl)),
        }
    }

    let started = Instant::now();
    let added = add_resolved(&elements);
    let elapsed = started.elapsed().as_millis() as u64;
    let addresses: Vec<Ipv4Addr> = elements.iter().map(|(address, _)| *address).collect();

    if let Err(error) = added {
        eprintln!("flow-sandbox-resolver: {error:#}");
        debug(args, &question, "servfail", &addresses, 0, elapsed);
        return Ok(Some(dns::respond_empty(
            query,
            &question,
            dns::RCODE_SERVFAIL,
        )));
    }
    debug(
        args,
        &question,
        "resolved",
        &addresses,
        elements[0].1,
        elapsed,
    );
    Ok(Some(response))
}

/// `nft` rather than a netlink library: one process per answered query, which
/// the spike measures and the runtime implementation can replace.
fn add_resolved(elements: &[(Ipv4Addr, u32)]) -> anyhow::Result<()> {
    let elements = elements
        .iter()
        .map(|(address, ttl)| format!("{address} timeout {ttl}s"))
        .collect::<Vec<_>>()
        .join(", ");

    let status = std::process::Command::new("nft")
        .args(["add", "element", "inet", "flow_sandbox", "resolved"])
        .arg(format!("{{ {elements} }}"))
        .status()
        .map_err(|e| anyhow::anyhow!("running nft: {e}"))?;

    if !status.success() {
        anyhow::bail!("nft add element resolved {{ {elements} }}: {status}");
    }
    Ok(())
}

fn forward(query: &[u8], upstream: SocketAddr) -> anyhow::Result<Option<Vec<u8>>> {
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| anyhow::anyhow!("binding: {e}"))?;
    socket.set_read_timeout(Some(UPSTREAM_TIMEOUT))?;
    socket
        .send_to(query, upstream)
        .map_err(|e| anyhow::anyhow!("sending to {upstream}: {e}"))?;

    let mut response = vec![0u8; BUFFER];
    match socket.recv_from(&mut response) {
        Ok((length, from)) if from == upstream => {
            response.truncate(length);
            Ok(Some(response))
        }
        Ok((_, from)) => anyhow::bail!("answer from {from}, not {upstream}"),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => anyhow::bail!("receiving from {upstream}: {error}"),
    }
}

fn debug(
    args: &Args,
    question: &dns::Question,
    outcome: &str,
    addresses: &[Ipv4Addr],
    ttl: u32,
    nft_ms: u64,
) {
    if !args.debug {
        return;
    }
    let addresses = addresses
        .iter()
        .map(Ipv4Addr::to_string)
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "flow-sandbox-resolver: name={} qtype={} outcome={outcome} addresses=[{addresses}] \
         ttl={ttl} nft_ms={nft_ms}",
        question.name, question.qtype
    );
}

fn parse(argv: Vec<String>) -> Result<Args, String> {
    let mut policy = None;
    let mut listen = None;
    let mut upstream = None;
    let mut debug = false;

    let value = |i: usize| -> Result<&str, String> {
        argv.get(i + 1)
            .map(String::as_str)
            .ok_or_else(|| format!("{} requires a value; {USAGE}", argv[i]))
    };
    let endpoint = |i: usize| -> Result<SocketAddr, String> {
        let raw = value(i)?;
        raw.parse()
            .map_err(|e| format!("{} {raw:?}: {e}; {USAGE}", argv[i]))
    };

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--policy" => (policy, i) = (Some(PathBuf::from(value(i)?)), i + 2),
            "--listen" => (listen, i) = (Some(endpoint(i)?), i + 2),
            "--upstream" => (upstream, i) = (Some(endpoint(i)?), i + 2),
            "--debug" => (debug, i) = (true, i + 1),
            other => return Err(format!("unrecognized argument {other:?}; {USAGE}")),
        }
    }

    let missing = |flag: &str| format!("{flag} is required; {USAGE}");
    Ok(Args {
        policy: policy.ok_or_else(|| missing("--policy"))?,
        listen: listen.ok_or_else(|| missing("--listen"))?,
        upstream: upstream.ok_or_else(|| missing("--upstream"))?,
        debug,
    })
}
