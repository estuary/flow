# locate-bin

Finds a companion executable for the running program: first beside it (the
directory of `argv[0]`), then on `$PATH`. Installed images co-locate their
binaries, so the sibling wins there.

- `locate`: the sibling file or first PATH match. Used for `sops`, `flowctl`
  and local commands, which run on this host.
- `locate_static`: the same order, but passes over dynamically linked ELF
  executables (those naming a `PT_INTERP` loader). Used for
  `flow-connector-init`, which runs inside a connector's container or VMM
  guest against that image's libc. A workspace `target/debug` holds a glibc
  build that is both beside `flowctl` and first on a mise `$PATH`; the static
  musl build lives in `target/<arch>-unknown-linux-musl/debug`, later on it.
  Anything not identified as a dynamically linked ELF, such as a test's
  stand-in script, resolves as it would through `locate`.
