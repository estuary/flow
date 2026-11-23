# locate-bin

Finds a companion executable for the running program: first beside it (the
directory of `argv[0]`), then on `$PATH`. Installed images co-locate their
binaries, so the sibling wins there.

- `locate`: the sibling file or first PATH match. Used for `sops`, `flowctl`,
  `flow-connector-init` and local commands.
