//! Fill the real nftables set and prove overflow fails closed without killing
//! the resolver. Reclamation and CNAME capacity use controlled state in unit tests.

use super::*;

#[test]
fn dns_capacity() {
    const CAP: usize = 1024;
    let run = run::load();
    let nameserver = scripted(&run, run.endpoints[0]);
    let refused = listen(&run, run.endpoints[1]);
    let addresses: Vec<Ipv4Addr> = (0..CAP)
        .map(|index| Ipv4Addr::from(u32::from(Ipv4Addr::new(192, 31, 200, 0)) + index as u32))
        .collect();
    let mut spec = run::Spec::probes(&run, "capacity.json");
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    // Fill addresses are never contacted, and cannot escape if a probe changes.
    run::nsenter(
        run::pid(&vmm),
        &["ip", "route", "add", "blackhole", "192.31.200.0/21"],
    );

    for (index, batch) in addresses.chunks(64).enumerate() {
        let name = format!("fill-{index}.acmeco.example");
        dns::set(
            &nameserver,
            &name,
            batch.iter().map(|ip| dns::Record::A(*ip, 3600)).collect(),
        );
        let answer = query(&mut guest, "fill", &name, 1);
        assert_eq!(answer["status"], "ok", "{name}: {answer}");
        assert_eq!(answer["records"].as_array().unwrap().len(), batch.len());
    }
    let expected: BTreeSet<_> = addresses.iter().copied().collect();
    assert_eq!(
        host::resolved(&vmm.name)
            .iter()
            .map(|e| e.addr)
            .collect::<BTreeSet<_>>(),
        expected
    );

    dns::set(
        &nameserver,
        "overflow.acmeco.example",
        vec![
            dns::Record::A(addresses[0], 3600),
            dns::Record::A(run.endpoints[1], 3600),
        ],
    );
    let answer = probe(
        &mut guest,
        "overflow",
        "query",
        serde_json::json!({
            "nameserver": VMM_TAP.to_string(), "name": "overflow.acmeco.example",
            "qtype": 1, "timeout": BLOCKED, "raw": true
        }),
    );
    assert_eq!(answer["status"], "rcode:2");
    // The probe returns only the status for errors; inspect ANCOUNT on the wire.
    assert_eq!(
        answer["raw"].as_str().and_then(|raw| raw.get(12..16)),
        Some("0000")
    );
    assert_eq!(
        host::resolved(&vmm.name)
            .iter()
            .map(|e| e.addr)
            .collect::<BTreeSet<_>>(),
        expected
    );
    assert_eq!(
        tcp(
            &mut guest,
            "overflow-blocked",
            refused.addr.ip(),
            refused.addr.port(),
            BLOCKED
        ),
        "timeout"
    );
    assert!(endpoint::accepted(&refused).is_empty());

    // Existing addresses still refresh at capacity, and the resolver still serves.
    assert_eq!(
        query(&mut guest, "refresh", "fill-0.acmeco.example", 1)["status"],
        "ok"
    );
    run::assert_framing(&vmm, true);
}
