# connector-vmm-tests

Opt-in tests that boot real guests on the reference VMM launch line. They prove
network policy, DNS authorization, image accounts, read-only image roots,
writable scratch, vsock RPC and enforcement outside a compromised VMM.

`mise run ci:connector-vmm-kvm` owns setup and teardown. It requires x86_64 KVM,
passwordless sudo, rootful podman, docker buildx, Python, musl-tools and
conntrack. Missing prerequisites fail the task rather than silently skip tests.
The normal nextest profile excludes privileged tests and runs harness unit tests.

## Roadmap

- `tests/kvm.rs`: guest behavior, isolation, host boundary tampering and reinstalls.
- `src/launch.rs`: reference network, launch and host boundary commands with snapshots.
- `src/run.rs`: staged run configuration, resource tracking and VMM lifecycle.
- `src/host.rs`: host observations of mounts, descriptors, conntrack and nftables.
- `src/guest.rs`, `fixtures/probes.py`: guest probe RPC.
- `src/dns.rs`, `src/endpoint.rs`, `src/netns.rs`: controlled nameservers and endpoints.
- `fixtures/guest.Containerfile`: the probe image based on derive-python.

The task owns the host boundary and controlled endpoint namespace for one run.
Each guest has its own network and state directory. Test-name filters are partial
runs; the task records resources before creation so interrupted runs can clean up.
