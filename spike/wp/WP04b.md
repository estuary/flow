# WP04b: podman's writable layer instead of a guest overlay

## Goal
Remove the guest-side overlayfs and the libkrun patch it required. The
writable root becomes what a container has today: podman's own per-container
writable layer over the image, served to the guest read-write over virtiofs.

## Read
- STATUS.md: the WP04 entry (what the overlay cost to build and why), the
  WP08b entry (the deps disk you must keep working, its 11 flow-init-test
  assertions, and the `mapfile` and `CONNECTORS+=` traps), and the master
  entry that chose this option.
- CONTRACTS.md: "Helper CLI", "flow-init", "Paths and names" (all rewritten
  for this shape).
- PLAN.md: "Helper launch", "Helper image", "flow-init", experiment 11.
- `spike/flow-init/src/root.rs` and `main.rs`; `spike/helper/Dockerfile`;
  `spike/helper/shim/src/main.rs` (the root share call and the overlay-dir
  injection); `spike/tasks/flow-init-test.sh`, `helper-smoke.sh`,
  `helper-common.sh`.

## May touch
`spike/flow-init/**`, `spike/helper/**`, `spike/tasks/helper-*.sh`,
`spike/tasks/flow-init-*.sh`, `spike/tasks/exp5-*.sh` (only the `--upper-mib`
argument and the image-mount option).

## Steps
1. Launch line: the image mount becomes
   `--mount type=image,source=<img>,destination=/rootfs,rw=true`. Podman
   creates a per-container writable overlay of the image and removes it with
   the container. Update `helper-common.sh` (or wherever the tasks build the
   mount) and verify `rw=true` is honored through the API service via
   `fake-reactor.sh`, not just `sudo podman` on the host.
2. Shim: root share is `krun_add_virtiofs3(ctx, "/dev/root", "/rootfs", 0,
   read_only=false)`. Drop the `/dev`, `/proc`, `/sys` overlay-dir injection
   if libkrun's init can now `mkdir` them itself on the writable share; keep
   it only if a busybox or distroless boot proves it still necessary, and
   record which. Remove the `--upper-mib` flag and its plumbing.
3. Dockerfile: delete the libkrun patch block (the `grep`/`sed` on
   `passthrough.rs`). libkrun stays v1.19.4 built from source, unpatched.
4. flow-init: delete the overlay entirely: no `unshare`, no private
   propagation, no tmpfs, no `pivot_root`, no `/dev` remount. Write
   `/etc/resolv.conf` and `/etc/hosts` straight into the root; `mkdir -p
   /venv /scratch` (and `/opt/venv` when `--deps-dev` is given) likewise.
   Everything else stays: eth0, IPv6 off, venv mount, scratch mount and
   chown, WP08b's `root::mount_deps`, `--as-root-exec`, env, uid drop, exec.
   Remove `--upper-mib` from the CLI.
5. Tests. `flow-init-test.sh` asserts: `/` is `virtiofs rw`; as root a write
   under `/etc` succeeds; as the image's user a write under `/etc` is EACCES
   and a write under `/tmp` succeeds (container parity, as WP04 established);
   exit codes still propagate (`--exec /nonexistent` is 127, the trap WP04
   found is gone with the pivot but assert it anyway); and after the helper
   exits, `podman image mount <img>` shows none of the guest's writes and
   `podman system df` is back to its pre-run size (the upper was removed with
   the container). `helper-smoke.sh` still green, all 41 `flow-init-test.sh`
   assertions still green including WP08b's deps ones. Then restate the
   experiment 5 number on this root shape:
   `exp5-run.sh --out exp5-4b.csv --cells blk-ext4,baseline --runs 10`.
   Removing the guest overlay takes a layer off the root's read path, so the
   2.12x should hold or improve; record it either way in `report/exp5.md` as a
   short "5c" section, without rewriting 5 or 5b.

## Verification
```
spike/tasks/helper-build.sh && spike/tasks/helper-smoke.sh && spike/tasks/flow-init-test.sh
spike/tasks/flow-init-test.sh --host
```
Record in STATUS: whether the overlay-dir injection was still needed; the
libkrun build is unpatched; `podman system df` before/after; the 5c number.
While you are in `helper-smoke.sh`, fix its `CONNECTORS+=` subshell bug the
same way WP08b fixed `flow-init-test.sh`; it is one line and you are
touching the file anyway.

## Out of scope
The deps disk (WP08b). Capping root writes: they are bounded by host disk,
as containers are today, and PLAN's experiment 11 now says so.
