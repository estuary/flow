# spike/helper

The sandbox helper: a container image that boots a connector image as a libkrun
microVM. The reactor launches one of these per connector instead of the
connector itself; see PLAN.md "Helper launch" and CONTRACTS.md "Helper CLI".

## Layout

- `Dockerfile`   builds `localhost/flow-sandbox-helper:spike`. Fedora 43, whose
                 `libkrun` (1.19.0) and `libkrunfw` (5.5.0) packages are used
                 as-is rather than built from source.
- `shim/`        the Rust shim, PID 1 of the helper container.
- `guest/`       code injected into the guest, so static and libc-free.
- `stubs/`       placeholders for binaries other packages own: `flow-init`
                 (WP04) and the two egress binaries (WP02).

Build with `spike/tasks/helper-build.sh`, smoke with `spike/tasks/helper-smoke.sh`.

## The shim

`shim/src/main.rs` is the whole sequence, top to bottom: network, disk, image
config, libkrun context, `krun_start_enter`. The other modules are the pieces it
calls, and none of them talk to libkrun except `sys`.

- `sys.rs`      libkrun's C ABI, transcribed from v1.19.4's header. The
                `krun-sys` crate is stuck at 1.10 and has none of these calls.
- `cli.rs`      the CONTRACTS CLI. Hand-rolled because `--exec` swallows every
                remaining argument.
- `image.rs`    `image-inspect.json` -> `/.krun_config.json`, plus `User`
                resolution against the image's own passwd/group files.
- `disk.rs`     the `O_TMPFILE` scratch disk and its `mkfs.ext4`.
- `net.rs`      the tap, and the egress binaries the shim execs before the VM.
- `console.rs`  console descriptors, and `--debug`'s tee.

## Non-obvious details

- **`Cmd`, never `krun_set_exec`.** libkrun's guest init consults
  `/.krun_config.json`'s `Cmd` only when `KRUN_INIT` is absent, and
  `krun_set_exec` is what sets `KRUN_INIT`. Setting both silently discards the
  config's argv, so the shim only ever writes `Cmd`.
- **The root share is read-only, so mount points cannot be created in it.**
  libkrun's init `mkdir`s `/dev`, `/proc` and `/sys` before mounting them;
  busybox has no `/proc` or `/sys` and distroless images have none of the three.
  The shim injects the missing ones as empty virtual directories.
- **hvc0 and `krun-stdout` share one host descriptor.** The kernel console and
  the workload's stdout are interleaved on fd 1 by construction; only the
  workload's stderr (fd 2, which the reactor reads) is separate.
- **Overlay-file memory is never freed.** libkrun serves those files straight
  out of the pointers it is given for the VM's whole life.
- **libkrun's vsock unix proxy has no half-close.** EOF on the host side of
  `/sock/init.sock` resets the connection rather than shutting down one
  direction.
