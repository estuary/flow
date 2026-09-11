# spike/catalog/

Catalog specs, fixtures, and policies the spike's `flowctl preview` runs drive.
Names are fictitious (`acmeCo/`) by the rule in `spike/README.md`.

Everything here is copied under `$SPIKE_REACTOR_DIR/wp06/` before a run: the
fake reactor mounts only that directory, `/run/podman` and `$CARGO_TARGET_DIR`,
so a spec left in the repo is not reachable from inside it. See
`spike_stage_catalog` in `spike/tasks/preview-common.sh`.

## Specs

- `capture-hello-world.flow.yaml` - a static Go capture, the only kind the stub
  helper can run. `rate: 2` plus a bounded `--sessions` makes the document count
  deterministic, so two previews can be diffed. Used by WP05's launch-line test
  and by experiments 1, 2 and 3.
- `materialize-sqlite.flow.yaml` - a static Go materialization, experiment 3's
  other half. Commits to a SQLite file at `/tmp/sqlite.db`, which under the
  switch is the guest's writable root. Needs `materialize-fixture.ndjson`.
- `derive-pandas.flow.yaml` plus `derive-pandas.flow.py` - experiment 4's Python
  derivation, whose pandas dependency `uv` must fetch from PyPI inside the
  guest. Needs `derive-fixture.ndjson`. Two non-obvious constraints, both
  derive-python's rather than the sandbox's: the module is type-checked by
  pyright in **strict** mode (hence the `pandas-stubs` dependency and the
  `Series.sum()` idiom), and it must never print to **stdout**, which is the
  derive protocol's own channel.

## Fixtures

`--fixture` feeds a materialization or derivation in place of live collection
data: newline-delimited `["collection/name", {...}]` documents separated by
`{"commit": true}` transaction markers.

## Policies

- `policy-egress-none.json` - the minimal valid policy of CONTRACTS "Policy
  JSON". No resolver is spawned. The switch requires the file whatever it says,
  since it copies it to `<id>/init/policy.json` for the helper.
- `policy-allow-all.json` - `egress: public`, `allowAll: true`, for experiment
  4. Under WP02's real ruleset this accepts all forwarded traffic with
  masquerade and anti-spoof still applied; under today's placeholder scripts it
  enforces nothing at all. See `spike/report/exp4.md`.
