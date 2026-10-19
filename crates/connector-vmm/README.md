# connector-vmm

Runs one OCI connector in a libkrun micro-VM. `flow-connector-vmm` prepares the
image, scratch disk, guest shares and network, then enters the VM. The connector
image and its root share are read-only; `guest-init` builds writable guest state
on scratch and starts the image's workload through `connector-init`.

## Roadmap

- `src/main.rs`: `run`, shared `check-launch` validation and `print-ruleset`.
- `src/image.rs`: image argv, environment and accounts, resolved inside the guest root.
- `src/launch.rs` and `src/krun.rs`: launch validation, descriptor ownership and libkrun calls.
- `src/disk.rs`: sparse ext4 scratch backing.
- `src/net.rs`: tap configuration, nameserver selection and IPv6 checks.
- `src/ruleset.rs`: nftables rules enforcing public destinations and DNS authorization.
- `src/resolver/`: DNS parsing, name admission and acknowledged set updates with TTL expiry.
- `src/console.rs` and `src/sys.rs`: connector logs and command execution.
- `fake-entrypoint.sh`: shared validation followed by a connector without KVM.

[`egress`](../egress/README.md) owns policy parsing and host-name semantics;
[`guest-init`](../guest-init/README.md) owns guest mounts and workload startup.
The runtime image and its fake are under `docker/connector-vmm*.Dockerfile`.

The VMM's own network rules are within its authority. Host enforcement outside
that authority is required before production launch. Unit tests cover policy,
DNS, launch configuration and image semantics; real guest behavior requires KVM.

## Host network boundary

`boundary install`, `boundary verify` and `boundary remove` run in the host's
network namespace. `src/boundary.rs` installs inet and bridge nftables tables
for bridges whose interface name begins `fvm`. Each VMM must have its own
network. Verify the boundary before creating it; installation with existing
unprotected VMM bridges fails and those VMMs must be replaced.

The host boundary keeps private destinations, host services, sibling VMMs,
IPv6, inbound connections and SMTP unreachable even after the VMM removes its
own rules. DNS name restrictions remain enforced inside the VMM. The opt-in
[`connector-vmm-tests`](../connector-vmm-tests/README.md) suite measures both.
