//! Refused DNS answers must not partially authorize addresses in the kernel.
//! TTL retention and CNAME expiry combinations are covered by resolver unit tests.

use super::*;

#[test]
fn dns_answer_changes() {
    let run = run::load();
    let [ns_ip, allowed_ip, other_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let forbidden_ip = run.decoys[0];
    let nameserver = scripted(&run, ns_ip);
    let allowed = listen(&run, allowed_ip);
    let other = listen(&run, other_ip);
    let forbidden = listen(&run, forbidden_ip);
    let name = "name.acmeco.example";
    dns::set(&nameserver, name, vec![dns::Record::A(allowed_ip, 3600)]);
    let mut spec = run::Spec::probes(&run, "capacity.json");
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);

    assert_eq!(query(&mut guest, "initial", name, 1)["status"], "ok");
    assert_eq!(
        probe(
            &mut guest,
            "hold",
            "hold",
            serde_json::json!({
                "id": "held", "addr": allowed_ip.to_string(), "port": allowed.addr.port(), "timeout": 30
            })
        ),
        "connected"
    );
    assert_eq!(
        probe(
            &mut guest,
            "initial-exchange",
            "exchange",
            serde_json::json!({"id": "held", "timeout": 30})
        ),
        "pong"
    );
    dns::set(
        &nameserver,
        "bad.cdn.example",
        vec![dns::Record::A(forbidden_ip, 3600)],
    );

    for (phase, records) in [
        ("forbidden", vec![dns::Record::A(forbidden_ip, 3600)]),
        (
            "mixed-held",
            vec![
                dns::Record::A(allowed_ip, 3600),
                dns::Record::A(forbidden_ip, 3600),
            ],
        ),
        (
            "mixed-new",
            vec![
                dns::Record::A(other_ip, 3600),
                dns::Record::A(forbidden_ip, 3600),
            ],
        ),
        (
            "mixed-reversed",
            vec![
                dns::Record::A(forbidden_ip, 3600),
                dns::Record::A(other_ip, 3600),
            ],
        ),
        (
            "cname",
            vec![dns::Record::Cname("bad.cdn.example".to_string(), 3600)],
        ),
    ] {
        dns::set(&nameserver, name, records);
        assert_eq!(
            query(&mut guest, phase, name, 1)["status"],
            "rcode:5",
            "{phase}"
        );
        assert_eq!(
            host::resolved(&vmm.name)
                .iter()
                .map(|element| element.addr)
                .collect::<Vec<_>>(),
            [allowed_ip],
            "{phase}: the kernel retains only the authorized address"
        );
        assert_eq!(
            probe(
                &mut guest,
                phase,
                "exchange",
                serde_json::json!({"id": "held", "timeout": 30})
            ),
            "pong",
            "{phase}: the established connection survives"
        );
    }
    assert_eq!(
        query(&mut guest, "target-refused", "bad.cdn.example", 1)["status"],
        "rcode:5"
    );
    assert!(
        !dns::queries(&nameserver)
            .iter()
            .any(|name| name == "bad.cdn.example")
    );
    for (label, listener) in [("other", &other), ("forbidden", &forbidden)] {
        assert_eq!(
            tcp(
                &mut guest,
                label,
                listener.addr.ip(),
                listener.addr.port(),
                BLOCKED
            ),
            "timeout"
        );
        assert!(
            endpoint::accepted(listener).is_empty(),
            "{label} received a connection"
        );
    }
    run::assert_framing(&vmm, true);
}
