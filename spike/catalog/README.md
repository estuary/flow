# spike/catalog/

Catalog specs and policies the spike's `flowctl preview` runs drive. Names are
fictitious (`acmeCo/`) by the rule in `spike/README.md`.

- `capture-hello-world.flow.yaml` - WP05's launch-line test. A static Go
  connector, the only kind the stub helper can run.
- `policy-egress-none.json` - the minimal valid policy of CONTRACTS "Policy
  JSON". The stub ignores it; the runtime switch still requires the file, since
  it copies it to `<id>/init/policy.json` for the helper.
