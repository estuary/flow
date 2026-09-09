# STATUS

Ledger for the spike. Sub-sessions append to the log; the master thread
maintains the table. Newest log entries at the bottom.

## Packages

| WP | Title                                   | Depends on   | Status | Branch              | Notes |
|----|-----------------------------------------|--------------|--------|---------------------|-------|
| 00 | Environment                             | -            | todo   | daveg/libkrun-spike |       |
| 01 | connector-init --vsock-port             | -            | todo   | daveg/libkrun-spike |       |
| 02 | Egress ruleset + resolver (netns)       | -            | todo   | daveg/libkrun-spike |       |
| 03 | Helper image + shim                     | -            | todo   | daveg/libkrun-spike |       |
| 04 | flow-init                               | 03 (to test) | todo   | daveg/libkrun-spike |       |
| 05 | runtime-next spike switch               | 01 (real)    | todo   | daveg/libkrun-spike |       |
| 06 | Integration: experiments 1-4            | 00-05        | todo   | daveg/libkrun-spike |       |
| 07 | Egress from the guest: experiments 6-8  | 02, 04, 06   | todo   | daveg/libkrun-spike |       |
| 08 | virtiofs import matrix: experiment 5    | 03, 04       | todo   | daveg/libkrun-spike |       |
| 09 | Churn and density: experiments 9-10     | 00, 06       | todo   | daveg/libkrun-spike |       |
| 10 | Storage, exposure, crash: exp 11-13     | 06, 11       | todo   | daveg/libkrun-spike |       |
| 11 | libkrun source read: experiment 12      | -            | todo   | daveg/libkrun-spike |       |
| 12 | Report                                  | all          | todo   | daveg/libkrun-spike |       |

Sequential order, one session at a time, biggest unknowns first:
00, 03, 04, 08, 01, 05, 06, 02, 07, 11, 10, 09, 12. (03 answers "does it
boot from an image mount here"; 08 answers "is virtiofs fast enough" before
any runtime work is spent.)

## Known unknowns (resolve and record here)

- Does `krun_set_exec` argv or `/.krun_config.json` `Cmd` win in libkrun
  1.19's init when both are present? (WP03)
- Is libkrun 1.19.x packaged for the helper's base image, or do we build it?
  (WP03)
- Does `vsock_loopback` load on this box, enabling a host-side connector-init
  vsock test? (WP00 checks, WP01 uses)
- What does the libkrun README's "does not provide any protection against the
  guest attempting to access other directories in the same filesystem" mean
  concretely for the read-only image share? (WP11)

## Log

<!-- Append entries below. Format:
### YYYY-MM-DD WPnn: <one-line outcome>
- shipped: ...
- verification: <command> -> <result>
- deviations from CONTRACTS.md: none | ...
- questions for master: none | ...
-->

### 2026-09-09 WP00: box provisioned; one command sets it up, one checks it.

- shipped:
  - `spike/tasks/env-common.sh` - names, subnets, image list, reactor dir,
    sourced by the other three scripts.
  - `spike/tasks/env-setup.sh` - idempotent. podman 4.9.3 + uidmap,
    slirp4netns, netavark, aardvark-dns from noble; rootful `podman.socket`;
    networks `flow-connectors` (10.89.0.0/24) and `spike-testnet2`
    (198.51.100.0/24); self-signed cert and two 1 KiB bodies generated into
    `spike/tasks/nginx/`; `spike-nginx` at 198.51.100.10 on 80 and 443;
    six images pulled; `/var/tmp/flow-spike/reactor` created and confirmed
    ext4; `vsock_loopback` modprobed.
  - `spike/tasks/env-check.sh` - read-only, one `FAIL` line per problem,
    exit 1 if any. Includes a live fake-reactor CapEff check.
  - `spike/tasks/fake-reactor.sh CMD...` - per CONTRACTS "Fake reactor".
    `--network=host --user root:root --no-hosts`, no `--cap-add`, no
    `--privileged`, no `--device`; `/run/podman`, `$SPIKE_REACTOR_DIR` at
    the same path, and the repo's `target/` at `/flow-target`.
  - `spike/tasks/nginx/nginx.conf` committed; cert, key and `www/` are
    generated and gitignored.

- verification:
  ```
  $ spike/tasks/env-setup.sh && spike/tasks/env-check.sh
  ok    podman 4.9.3
  ok    /run/podman/podman.sock
  ok    /dev/kvm
  ok    /dev/net/tun
  ok    network flow-connectors
  ok    network spike-testnet2 198.51.100.0/24
  ok    http://198.51.100.10/probe -> 1024 bytes
  ok    https://198.51.100.10/probe -> 1024 bytes
  ok    https://198.51.100.10/ -> 1024 bytes
  ok    /var/tmp/flow-spike/reactor is ext4
  ok    fake reactor CapEff 00000000800405fb
  ok    vsock_loopback loaded

  env-check.sh: ok

  $ spike/tasks/fake-reactor.sh podman ps
  CONTAINER ID  IMAGE                             COMMAND   ...  NAMES
  75ba4423c99e  docker.io/library/nginx:latest    nginx ... ...  spike-nginx
  619144b6b043  ghcr.io/estuary/reactor:v0.6...   podman ps ...  vigorous_goldstine

  $ spike/tasks/fake-reactor.sh grep CapEff /proc/self/status
  CapEff:	00000000800405fb

  $ curl -sk https://198.51.100.10/probe | wc -c
  1024
  ```

- image digests (pulled 2026-09-09):
  ```
  ghcr.io/estuary/reactor@sha256:7f5d773418042d355fe5bd8fe3da262b2e679374b224bff055d8f6d369ab06f0
  ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7
  ghcr.io/estuary/source-hello-world@sha256:ebac9e3e9f16cec5d27d59de6e40186bab142fd93d3de0c4bfbea73e06b79941
  ghcr.io/estuary/materialize-sqlite@sha256:54d2853ccf0319e33ef122cd5f0db1b89c1f87d4fce7168d779204e0bece179f
  docker.io/library/busybox@sha256:1cfa4e2b09e127b9c4ed43578d3f3c18e7d44ea47b9ea98475c0cbe9086525f8
  docker.io/library/nginx@sha256:05b8cb60c354a44ab824ea6e7dc69b46d50762cdbe728a347a5b656e6fb3d7c4
  ```
  Reactor tag: `ghcr.io/estuary/reactor:v0.6.13-127-g6339ac87653`. Its config
  has no ENTRYPOINT (`Cmd=[/bin/bash]`, `WorkingDir=/tmp`), so
  `fake-reactor.sh CMD...` needs no `--entrypoint` override. It ships a podman
  client and coreutils.

- known unknowns resolved:
  - `vsock_loopback` loads on this box. WP01's host-side AF_VSOCK test of
    `flow-connector-init --vsock-port` is available.

- deviations from CONTRACTS.md: none. Two departures from the WP00 brief:
  - The brief says to confirm the reactor filesystem with `stat -f -c %T`.
    That reports ext4 as `ext2/ext3` (ext2/3/4 share a superblock magic), so
    both scripts use `findmnt -no FSTYPE -T`, which distinguishes them.
    `/var/tmp/flow-spike/reactor` is ext4 on `/dev/root`.
  - The brief points at the Quadlet `Image=` line for the reactor tag. There
    is no Quadlet on this box and PLAN's inputs do not quote the line, so
    env-common.sh pins the newest published tag (above) and honors a
    `SPIKE_REACTOR_IMAGE` override.

- questions for master:
  - Should the reactor image be repinned to production's actual tag? If the
    Quadlet `Image=` line is available, set `SPIKE_REACTOR_IMAGE` in
    `spike/tasks/env-common.sh`; nothing else needs to change.
  - `flow-connectors` got 10.89.0.0/24, inside the baseline denylist's
    10.0.0.0/8. That is production's shape, but it means WP02's "helper's own
    eth0 subnet" exception and the 10/8 baseline drop overlap. Confirm the
    exception is meant to be narrower than the drop (it must be, or the
    helper's uplink is unusable), and that WP02 orders them accordingly.
  - `docker.io/library/busybox` and `nginx` are pinned by tag, not digest.
    Fine for a spike, but experiment numbers will name the digests above.
