# Static outbound routing for VMM connectors

VMM connectors use separate bridge subnets. Host routing must direct their
outbound traffic through the data plane's designated NAT path while preserving
private routes. An isolated Linux/KVM lab reproduces a source-routing mismatch
and verifies a deployment candidate in `estuary/est-dry-dock`.

This document describes that candidate and its lab results, not an applied
production change.

## Authoritative deployment configuration

The data-plane controller's `job::Args` selects `estuary/est-dry-dock` as its
provisioning repository (`crates/data-plane-controller/src/job/mod.rs`). The
inspected deployment revision is `f034c7e44402fc0116b16ca9b3dd8796cf5160ac`.

- [99-nat-routes.j2](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/roles/common/templates/99-nat-routes.j2)
  declares source rules for `connector_bridge_cidr`. Priority 32000 selects
  main for the connector subnet and public subnet. Priority 32001 selects
  table 101 for other destinations. Table 101 defaults through
  `iface_nat_gateway` on `iface_nat_name`. Host masquerading happens later.
  Explicit `extra_private_route_cidrs` also get main-table routes.
- [common.yml](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/roles/common/tasks/common.yml)
  discovers the NICs and gateways, renders Netplan, and enables the boot-time
  `netplan-apply.service`.
- [podman-setup.yml](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/roles/reactor/tasks/podman-setup.yml)
  creates `flow-connectors` using that connector CIDR and gateway.
- [sysctl-hardening.j2](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/roles/hardening/templates/sysctl-hardening.j2)
  enables strict reverse-path filtering for all and newly created interfaces.
- [gcp.py](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/est_dry_dock/resources/gcp.py)
  `make_region_nat_network` creates two reserved addresses and a MANUAL_ONLY
  Cloud NAT for the private subnet. Reactors attach a second NIC to it.
- [aws.py](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/est_dry_dock/resources/aws.py)
  `new_zone_nat` allocates two EIPs for a NAT gateway and points the private
  subnet's default route at that gateway. Specific private routes coexist.
- [azure.py](https://github.com/estuary/est-dry-dock/blob/f034c7e44402fc0116b16ca9b3dd8796cf5160ac/est_dry_dock/resources/azure.py)
  `new_nat` creates two static public addresses; the private subnet attaches
  to that NAT gateway.

A plane can advertise multiple NAT addresses. Cloud validation must check
membership in its advertised address set, not require two connections to
select the same member.

## Packet paths and root cause

Ordinary connectors use the launcher's configured network
(`connector/src/container.rs`, `run`). Deployment configures this as
`flow-connectors`. VMM launch plans instead create one bridge per VMM,
`fvm<12 hex>`, with a dynamically allocated subnet (`connector/src/vmm/plan.rs`).
No VMM joins the ordinary bridge.

| Decision or translation | Ordinary connector | VMM connector |
| --- | --- | --- |
| Workload sends packet | Its `flow-connectors` address | Guest `192.0.2.2` through virtio-net/tap0 |
| Container forward filter | Engine/container path | `inet flow_egress`: DNS authorizations, exclusions, declared CIDRs |
| Container postrouting | No guest translation | Guest masqueraded to container eth0 address (`10.89.N.x` in lab) |
| Host bridge / prerouting | Source in `connector_bridge_cidr` | Source in the VMM bridge's distinct subnet |
| Host route lookup, original deployment | Priority 32001 -> table 101 for internet | No source-rule match -> main -> public NIC |
| Host postrouting | Netavark masquerades to private NIC address | Netavark masquerades to the NIC chosen by routing |
| Cloud edge | Private NIC -> static NAT address set | Public NIC -> its own public source address |

`connector-vmm/src/ruleset.rs` installs guest masquerading at postrouting
`srcnat` (100). Host routing therefore never sees `192.0.2.2`. Moving host
SNAT earlier in postrouting cannot fix a route already selected from the
container's source address. Netavark's host POSTROUTING chains also masquerade
at priority 100; conntrack and the lab's kernel rule listings confirm both
translations. The simulated cloud router sees a host NIC address and applies
the final public translation.

The host boundary's anti-spoof check runs at prerouting -300, before conntrack
(-200). Its bridge and inet filters remain responsible for VMM isolation.
Host ingress reaches a declared container TCP port, is DNATed inside the VMM
to the guest, and returns through conntrack's inverse translations. Its
host authorization mark is `0x46564d49`, cleared at filter priority -12,
conditionally assigned at -11, and checked by the boundary at -10.

## Deployment candidate

The candidate adds the following to the deployment repository:

- `roles/common/templates/flow-vmm-routing.j2`: a separate nftables table
  assigns conntrack bit `0x01000000` to original-direction packets entering
  reserved `fvm*` bridges at prerouting mangle (-150). Both directions restore
  that bit into the packet mark. Other mark bits are preserved.
- Priority 31998 looks up main with `suppress_prefixlength 0` for marked
  packets. Connected bridge routes and explicit private routes take precedence.
  Priority 31999 then selects the existing table 101. No cloud gateway,
  network pool, or bridge address is hard-coded in the fix.
- `src_valid_mark=1` on the NAT NIC lets strict reverse-path checks use the
  reply's restored routing bit. The connected-route lookup also keeps replies
  addressed to VMMs on their bridge. Reverse NAT happens before route lookup.
- `flow-vmm-routing.network.conf` keeps the same two RPDB rules in a networkd
  drop-in for the Netplan-generated NAT interface. Netplan reconfiguration
  therefore retains their ownership. The script's immediate installation is
  idempotent and does not delete unrelated rules or duplicate its own.
- Ansible installs nftables, the script, drop-in, and boot unit.
  `flow-vmm-routing.service` requires and follows `netplan-apply.service`.
  The reactor requires and follows routing on hosts with a NAT interface.

Netavark's observed host-port masquerade mark is `0x2000`. The routing bit
does not overlap it. No NAT rule is added or reordered by the fix.

The routing bit is distinct from the per-packet authorization mark and is not
an admission decision. CIDR grants can overwrite the packet mark in FORWARD,
after route lookup. The connection retains the routing bit; authorization
is never copied into conntrack. Revocation
still removes authorization for each packet, including established flows.
The fix adds no accepts, broadens no allowed destinations, and changes no VMM
bridge membership. Host-originated ingress is conntrack's original direction;
its replies entering a VMM bridge are not classified as VMM-originated egress.

## Reproduction and verification

Run with a deployment checkout containing the candidate:

```bash
mise run ci:connector-vmm-kvm --routing-deployment /path/to/est-dry-dock
```

An optional test-name filter still applies to the existing suite. The routing
lab runs completely whenever the deployment option is supplied.

`fixtures/routing.py` starts a rootful host Podman API daemon in its own host
network namespace, with independent storage and a UNIX API socket. All engine
launch calls use that socket. Public and private NICs lead to a separate
simulated cloud router and source-recording endpoint namespace. The lab uses
strict reverse-path filtering, actual netavark masquerading, the real VMM
boundary, actual KVM guests, and the rendered deployment routing script.
No lab route or firewall rule is installed in the machine's network namespace.

The lab is recorded in the task's resource journal. Its owner lock fences
recovery; a manifest records namespaces and daemon PIDs with process start
times. Normal failures clean up, and task teardown retries recorded recovery.
The test uses a dedicated host API daemon; it does not drive the machine's
existing `/run/podman/podman.sock` or a deployed reactor container.

| Endpoint observation | Before candidate | After candidate |
| --- | --- | --- |
| Ordinary internet connection | `192.31.196.250` | `192.31.196.250` |
| VMM internet connection | `192.31.196.251` | `192.31.196.250` |
| Private VMM to allowed `10.20.0.2` via explicit main route | Not part of baseline comparison | `10.0.0.2` (internal path, no internet NAT) |
| Unrelated container / host internet connection | Main/public path | `192.31.196.251` |

These are invented lab addresses. Host conntrack records the VMM bridge source
and private-NIC reply destination `10.1.0.2`, with routing mark 16777216.
The simulated cloud router records original source `10.1.0.2` and reply
destination `192.31.196.250`. VMM namespace conntrack records the original
guest source `192.0.2.2` and container-address reply destination.

The lab checks public unnamed and internal destinations are denied, private
`0.0.0.0/0` and `/32` allowances retain the explicit internal route, revoked
host CIDR grants block traffic, host ingress works, VMMs cannot reach each
other even with `0.0.0.0/0`, unrelated traffic keeps its path, concurrent
launches and teardown preserve other VMMs, routing reinstallation recognizes
networkd-managed rules without duplication,
and API restart/network reload preserves the static path.

Validation results: 194 default tests passed across `connector`,
`connector-vmm`, `connector-vmm-tests`, and `egress`; 63 KVM/launcher tests
passed; connector-init's separate vsock test passed. Additional filtered
`egress_every_address` and `egress_cidrs` runs passed with the routing lab.
The release suite includes a failure/retry snapshot for routing-lab cleanup.
Killing a live lab owner with SIGKILL also verified actual journal-driven
recovery: no lab namespaces, engine containers, store, or journal entries
remained. Shell/YAML/Python syntax, real Netplan generation, and systemd unit
dependency validation passed.

## Rollout and remaining production verification

Provision this in the deployment repository before enabling VMMs. Drain
existing VMMs first: their established conntrack NAT mappings refer to the
previous NIC. Provision routing and its reactor dependency, verify table 101,
the reserved mark/rule priorities, and the boundary, then recreate VMMs.
Ordinary source rules and cloud NAT resources require no change.

The packet-path fix is lab-proven. The inspected infrastructure source was
available; no live data-plane inventory, deployed unit graph, or cloud plane
was inspected or changed. A canary must verify actual advertised-address
membership, real PrivateLink/TGW/customer routes, UFW and other host-policy
interaction, and full reboot/DHCP/Netplan reconfiguration. Unit dependency
validation and Netplan generation were tested in a temporary filesystem;
an actual machine reboot was not performed. IPv6 ordinary-connector behavior
is outside this IPv4 NAT change; VMM IPv6 remains blocked.
