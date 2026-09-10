# First-class secrets, end to end

A self-checking catalog under the `test/secrets/` prefix which proves the whole
secrets chain against a local stack: `flowctl secret set` wraps a value through
config-encryption and stores it, a task names it in a `secrets` stanza, and the
runtime resolves and merges it as the connector starts.

Two sibling tasks each draw on a sibling secret, and each asserts a different
half of the chain:

- **`test/secrets/hello`**, a `source-hello-world` capture, publishes `rate: -1`
  in the clear — a value the connector rejects outright. The sibling secret
  `test/secrets/rate` overrides it to `1`, so the capture *running at all*
  proves the merge happened. It writes greetings into `test/secrets/pings`.
- **`test/secrets/probe`**, a Python derivation reading `pings`, holds two
  environment variables: `EXPECTED_TOKEN`, published in the clear, and
  `ACTUAL_TOKEN`, which appears in the configuration **only** as the resolved
  value of the sibling secret `test/secrets/token`. `probe.py` compares them and
  publishes `{ok: true}` (or `{ok: false}` with a length and digest — never the
  value), so a derived document is the assertion.

The two secrets cannot be one entity: `rate` is a JSON number, while
`ACTUAL_TOKEN` lands in a string-valued environment map. So the catalog carries
one of each type, and covers both a number-valued and a string-valued secret.

The capture is also the derivation's **driver**: published together, the
greetings it captures are what invoke the derivation's transform, so the live
pair exercises the runtime rather than a fixture.

Two authorization paths reach the same resolution code, and both are exercised
below:

- **User** — `flowctl preview` runs the connector locally and resolves under the
  logged-in user's JWT, through `/authorize/user/decrypt-secret`.
- **Task** — a publication's Validate runs in the data plane, whose reactor
  signs a data-plane JWT naming the task and the secret, through
  `/authorize/task/decrypt-secret`.

## Layout

- `flow.yaml` — the `test/secrets/hello` capture, the `test/secrets/pings`
  collection between them, and the `test/secrets/probe` derivation.
- `probe.py` — the derivation module, which is the comparison.
- `pings.fixture.jsonl` — two source documents in two transactions, fed to
  `flowctl preview --fixture` so the derivation can be previewed without
  running the capture and without live journal data.

On the derivation, `enable-runtime-v2: "true"` is load-bearing: secrets are
resolved by the V2 connector start pipeline, and publication rejects a stanza on
a V1 task. `derive-image-tag: local` runs the locally-built connector image.
The capture needs neither flag — captures are already V2 by default, and it
names its image outright.

## Running it

Bring up the stack and provision the `test/` tenant, then export its
credentials (see `local/README.md`):

```bash
mise run local:stack && mise run local:test-tenant
source ${FLOW_STACK_DIR}/test-tenant-test.env
```

### 1. Set the secrets, and read them back

`set` takes its value from stdin or `--from-file`, never from an argument, and
treats it as a JSON string unless `--json` is given. The rate is a number, so it
needs `--json`; the token is a string, so it must not have it.

```bash
echo -n "s3cr3t-t0ken" | mise exec -- flowctl secret set test/secrets/token
echo -n "1" | mise exec -- flowctl secret set test/secrets/rate --json
mise exec -- flowctl secret list test/secrets/
mise exec -- flowctl secret decrypt test/secrets/rate
```

`decrypt` requires the `DecryptSecret` capability, which `list` does not.

### 2. Publish — the task path

```bash
mise exec -- flowctl catalog publish --source tests/secrets/flow.yaml --auto-approve
```

The build log carries each connector's own record of its resolution:

```
INFO: resolved task secret  secret="test/secrets/rate" secretId="..."
INFO: resolved task secret  secret="test/secrets/token" secretId="..."
```

Those lines are emitted by the connector start pipeline in the data plane, so
publishing at all proves the reactor's data-plane JWT was accepted by
`/authorize/task/decrypt-secret` and that config-encryption unwrapped the
documents. For the capture it proves more: its Validate would have failed on the
plaintext `rate: -1` had the merge not happened.

### 3. Preview the capture — the number-valued secret

```bash
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
  --name test/secrets/hello --sessions 3
```

```json
["test/secrets/pings",{"message":"Hello 0!","ts":"...:25.499786894Z"}]
["test/secrets/pings",{"message":"Hello 1!","ts":"...:26.502735398Z"}]
["test/secrets/pings",{"message":"Hello 2!","ts":"...:27.504188279Z"}]
```

The one-second spacing is the assertion: the merged `rate` is `1`, not the `-1`
the specification publishes.

### 4. Preview the probe — the user path, and the value itself

```bash
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
  --name test/secrets/probe --fixture tests/secrets/pings.fixture.jsonl
```

```json
["test/secrets/probe",{"ts":"2026-01-01T00:00:00Z","message":"Hello 0!","ok":true,"detail":null}]
["test/secrets/probe",{"ts":"2026-01-01T00:00:01Z","message":"Hello 1!","ok":true,"detail":null}]
```

### 5. Pipe them together

`--fixture` accepts `-` (or a FIFO) and streams, and capture preview already
writes the fixture's `["collection",<doc>]` document form. Only the
`{"commit": true}` transaction markers are missing, which `awk` supplies —
`fflush()` because awk block-buffers into a pipe:

```bash
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
    --name test/secrets/hello --sessions 3 \
  | awk '{print; print "{\"commit\": true}"; fflush()}' \
  | mise exec -- flowctl preview --source tests/secrets/flow.yaml \
      --name test/secrets/probe --fixture -
```

```json
["test/secrets/probe",{"ts":"...:42.865759235Z","message":"Hello 0!","ok":true,"detail":null}]
["test/secrets/probe",{"ts":"...:43.869488915Z","message":"Hello 1!","ok":true,"detail":null}]
["test/secrets/probe",{"ts":"...:44.870492504Z","message":"Hello 2!","ok":true,"detail":null}]
```

Both connectors resolve their own secret in the same shell, one process each.
Without the `awk` this still works: documents with no trailing commit marker
form a single final transaction.

### 6. The live pair

Published, the capture drives the derivation through real journals at one
document per second:

```bash
mise exec -- flowctl collections read --collection test/secrets/probe --since 10m --follow
```

```json
{"message":"Hello 7!","ok":true,"detail":null,"ts":"...:01.729666847Z"}
{"message":"Hello 8!","ok":true,"detail":null,"ts":"...:02.734130463Z"}
```

Reads of a live data plane need the stack's CA on `SSL_CERT_FILE`; see
`mise run local:stack-info`.

Note that the capture then runs until you delete it, accumulating roughly 86k
documents a day in `pings` and as many again in `probe`. Nothing prunes them;
delete the tasks when you're done:

```bash
mise exec -- flowctl catalog delete --prefix test/secrets/ --dangerous-auto-approve
```

### 7. Rotate, and see the lazy pickup

A running task keeps the value it resolved at start; a new one picks up the
current secret. Rotate the token without updating `EXPECTED_TOKEN` and the next
preview reports the divergence:

```bash
echo -n "rotated-t0ken" | mise exec -- flowctl secret set test/secrets/token
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
  --name test/secrets/probe --fixture tests/secrets/pings.fixture.jsonl
```

```json
["test/secrets/probe",{"ts":"2026-01-01T00:00:00Z","message":"Hello 0!","ok":false,
  "detail":"actual(len=13 sha256=84125a2bdc7a) != expected(len=12 sha256=fe5f98c6100c)"}]
```

Restore it with the original value to go green again.

### 8. Delete, and see the dangling reference

Publication does not check that a referenced secret exists; resolution does,
and names both the secret and the configuration locations it serves:

```bash
mise exec -- flowctl secret delete test/secrets/rate
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
  --name test/secrets/hello --sessions 3
```

```
failed to resolve secret 'test/secrets/rate', used at configuration location(s) /rate: control-plane API responded 404: secret 'test/secrets/rate' does not exist (NotFound)
```

The context names the stanza entry and the status carries the cause. Wrapping a
`tonic::Status` in context does not change its outcome, so `anyhow_to_status`
keeps the `NotFound` and folds the context into its message -- rather than
returning the status verbatim, which used to drop the context with it.

### 9. The publication guards

Each of these is rejected before any connector runs. Copy `flow.yaml` aside and
edit it to see them:

| Edit | Error |
| --- | --- |
| `enable-runtime-v2: "false"` on the derivation | uses \`secrets\`, which requires the V2 task runtime — reported alongside the same error for \`environment\` |
| secret `test/other/rate` on the capture | may use only secrets which are its siblings, but secret test/other/rate is not directly under the prefix test/secrets/ |
| a top-level `sops` key in the capture config | uses \`secrets\`, but its endpoint configuration has a top-level \`sops\` property |

Each message is prefixed with the entity it concerns, e.g. `derivation
test/secrets/probe ...` or `capture test/secrets/hello ...`, and the runtime-v2
error goes on to name the flag to set.

One further edit is rejected by the *connector*, not publication, and is the
capture's assertion stated in the negative — drop its `secrets` stanza and the
plaintext value comes back at you:

```
parsing endpoint config: message rate must be positive (got -1.000000)
```

Nothing in the control plane validates an endpoint configuration against its
connector's schema: the schema is consulted only to find plaintext secrets
(`crates/validation/src/secrets/plaintext.rs`), so `-1` reaches the connector
untouched. That is what makes the merge, and only the merge, load-bearing.

## What this catalog does not cover

- **The capture secret's value.** `source-hello-world` composes its message from
  the *resource* config, while `secrets` merges only into the *endpoint* config
  (`models::CaptureDef::secrets`), and its endpoint config is `{rate}` alone,
  parsed with `DisallowUnknownFields`. So the capture's secret is asserted by
  liveness rather than by comparison. Injecting a value a capture then echoes
  into its documents needs an endpoint-config field which no in-repo test
  connector currently has.
- **The plaintext invariant.** `derive-python` advertises a permissive `{}`
  configuration schema and `source-hello-world` marks nothing `secret: true`, so
  neither annotates a location the check can fire on. Exercise it against an
  image connector whose schema marks a credential secret — a `source-postgres`
  capture with a plaintext `password` beside a stanza is refused at publication.
- **Materializations.** The resolution path is shared by all three task types
  (`crates/connector/src/protocol.rs`) and the validation checks are per-type
  and covered by `crates/validation/tests/secrets_tests.rs`; this catalog
  exercises a capture and a derivation.
