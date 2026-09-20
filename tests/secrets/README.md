# First-class secrets, end to end

A self-checking catalog under the `test/secrets/` prefix, and the **reference
connector** which drives it. Together they cover the whole of first-class
secrets against a local stack:

- an operator sets a secret (`flowctl secret set`), and a task names it in a
  `secrets` stanza which the runtime resolves and merges as the connector starts;
- a secret which is **not** a sibling of the task reaches it anyway, because the
  **image rule** admits a secret named for the connector's own image repository;
- a connector **migrates itself** off a legacy `sops` configuration onto
  first-class secrets, without a human in the loop;
- and then **rotates** the credential it manages, on its own schedule.

The first two are visible in a single publication. The last two are what
`connector/` exists to demonstrate, and are the parts a connector author is
actually here to copy.

## The cast

| Thing | What it is | Why |
| --- | --- | --- |
| `connector/` | `source-secrets`, a Pydantic-only Python capture connector | The reference implementation of migration and rotation |
| `test/secrets/source` | A capture running that image | Migrates, then rotates, writing one document per tick |
| `test/secrets/events` | Its target collection | Carries `generation` and `source`, never a credential |
| `test/secrets/probe` | A Python derivation over `events` | Asserts *its own* secret resolved, and echoes the capture's |

### The secrets

Four, deliberately of different kinds and reached by different rules:

| Secret | Type | Rule | Who writes it |
| --- | --- | --- | --- |
| `test/connectors/registry.example/acme/source-secrets/oauth-client` | object | **image** | an operator, by hand |
| `test/secrets/oauth-tokens` | object | sibling | **the connector** |
| `test/secrets/rotate-every` | number | sibling | an operator, by hand |
| `test/secrets/token` | string | sibling | an operator, by hand |

The first is the interesting one. `test/connectors/` is not a prefix
`test/secrets/source` is a sibling of, and no sibling rule could ever admit it.
The rightmost `connectors` component is a reserved delimiter: everything after
it except the leaf must exactly equal the image repository,
`registry.example/acme/source-secrets`. The repository itself therefore cannot
contain a `connectors` component. The image must also *declare* the full secret
name in its `dev.estuary.secrets` label, together with the configuration
location it merges at, and the task's stanza must name that same location.
This is how a vendor's OAuth client secret reaches the tasks of every tenant
using that connector, without being copied into each of them -- and without
any of them choosing where it lands.

The other two object/number/string kinds are there so the catalog exercises a
secret of each JSON type: `rotate_every` is a number, `token` a string, and the
credential secrets are objects merged over one location.

## Layout

```
connector/
├── Dockerfile                    # image labels, incl. dev.estuary.secrets
└── source_secrets/
    ├── models.py                 # wire types; `secret: true` lives here
    ├── rotation.py               # the three HTTP calls, and nothing else
    └── __main__.py               # serve loop, migration, rotation
flow.yaml                         # the catalog, pre-migration
probe.py                          # the derivation module
events.fixture.jsonl              # source documents for `flowctl preview --fixture`
```

`rotation.py` is the file to read first if you are migrating a connector: it is
the entire client surface.

## How the connector is handed its credential

The reactor provides every connector with a **connector mount**, named by
`$CONNECTOR_MOUNT`, for file resources. Where task update is available
it contains `task-update.json`:

```json
{"token": "…", "control_plane_url": "…", "config_encryption_url": "…"}
```

`token` bears `TASK_UPDATE` scoped to this task and is regularly re-written by
the reactor. Local contexts like `flowctl preview` omit this file.
`crates/connector/README.md`, under "Connector mount", is the normative
contract.

## Building the connector

There is no mise task; this image is built by a person working through this
file, not by CI.

```bash
docker build --platform linux/amd64 -t registry.example/acme/source-secrets:local tests/secrets/connector
```

`--platform` is not optional on an arm64 host: the runtime runs every connector
as `linux/amd64`, and would try to *pull* an image it can't find in that
platform — from a registry which does not exist.

```bash
mise exec -- local/install-connector.sh registry.example/acme/source-secrets
```

The second command is not optional: publication refuses an image with no
`connectors` / `connector_tags` row ("Forbidden connector image"). The registry
is fictitious on purpose — nothing should ever try to pull this — and the
`:local` tag is what tells the agent not to. It runs under `mise exec` because
the script reaches this checkout's stack through variables that are ambient
only under mise.

**An image is bound to the prefixes its label names.** `dev.estuary.secrets`
is a JSON object of full secret catalog names to JSON pointers, and the reactor
checks *every* name against the image rule before the container runs — whether
the running task uses it or not. A typo there fails at start, loudly, rather than at the moment
some task first depends on it. Re-point the label and you must rebuild.

## Running it

Bring up the stack and provision the `test/` tenant, then export its
credentials (see `local/README.md`). The `source` has to happen in your own
shell, which is why the stack's variables are made ambient first rather than
the line being run under `mise exec`:

```bash
mise run local:stack && mise run local:test-tenant
```

```bash
eval "$(mise run local:stack-env)" && source "${FLOW_STACK_DIR}/test-tenant-test.env"
```

### 1. Set the three operator-owned secrets

`set` takes its value from stdin or `--from-file`, never from an argument, and
treats it as a JSON string unless `--json` is given.

```bash
echo -n "s3cr3t-t0ken" | mise exec -- flowctl secret set test/secrets/token
```

```bash
echo -n "30" | mise exec -- flowctl secret set test/secrets/rotate-every --json
```

```bash
echo '{"client_id":"vendor-client-id","client_secret":"vendor-client-secret"}' \
  | mise exec -- flowctl secret set \
      test/connectors/registry.example/acme/source-secrets/oauth-client --json
```

Deliberately *not* the `acme-client-id` that `flow.yaml` carries in the clear.
The two are meant to differ, so that `client_id_digest` in the captured
documents says which one is in force.

```bash
mise exec -- flowctl secret list test/
```

`test/secrets/oauth-tokens` is absent, and stays absent: the connector creates
it during migration. That is the point of the exercise.

### 2. Note what publication does to the configuration

`flow.yaml` carries its four credentials in the clear so the file is readable.
What gets *stored* is not: publication seals an endpoint configuration through
config-encryption, so the published model is a `sops` envelope under the
stack's age key (`mise/tasks/local/control-plane` holds the development key).
That is exactly the shape a task published years ago has, and nothing extra is
needed to reproduce it.

Note the capture has **no** `secrets` stanza. An empty stanza beside a sealed
configuration is what a pre-migration task looks like, and is what the
connector keys its migration off.

### 3. Publish

```bash
mise exec -- flowctl catalog publish --source tests/secrets/flow.yaml --auto-approve
```

The build log carries the derivation's own record of its resolution
(abbreviated; logs are JSON documents):

```json
{"level":"info","message":"resolved task secret","fields":{"secret":"test/secrets/token","secretId":"..."}}
```

That line is emitted by the connector start pipeline in the data plane, so
publishing at all proves the reactor's data-plane JWT was accepted by
`/authorize/task/decrypt-secret` and that config-encryption unwrapped the
document. The capture emits no such line yet: its configuration is still
sealed, and it names no secrets.

### 4. Watch the migration

```bash
mise exec -- flowctl collections read --collection test/secrets/events --since 10m --follow
```

```json
{"ts":"...:25Z","generation":0,"source":"legacy","client_id_digest":"2b42c87788c0","token_digest":"012df9eada1f"}
{"ts":"...:30Z","generation":0,"source":"sibling","client_id_digest":"d6b12258b72f","token_digest":"012df9eada1f"}
{"ts":"...:35Z","generation":0,"source":"sibling","client_id_digest":"d6b12258b72f","token_digest":"012df9eada1f"}
```

`source` flips from `legacy` to `sibling` **exactly once**, and that flip is the
assertion. What happened between those documents:

1. The connector saw an empty `secrets` stanza on `Open`, so it wrapped
   `{access_token, refresh_token}` through
   `POST <config_encryption_url>/secret/encrypt` and stored the result with
   `POST <control_plane_url>/task/set-secret`.
2. It then called `POST <control_plane_url>/task/update-config` with a plaintext
   configuration and the three-entry stanza.
3. That route does not publish. It records the update and wakes the task's
   controller, which publishes it — and **that publication restarts the
   connector**. The restarted session sees a non-empty stanza.

`client_id_digest` changing in the same document is the image rule landing.
Before the flip the client id is the one `flow.yaml` carried in the clear;
after it, the task's configuration has no `credentials` at all, and the value
arrives only because the vendor's secret — which no sibling rule could ever
admit — merged at `/credentials`.

The task's ops logs carry both sides:

```bash
mise exec -- flowctl logs --task test/secrets/source --since 10m
```

Abbreviated to their messages and the fields that matter, in order:

```json
{"message":"opened","fields":{"source":"legacy","generation":0}}
{"message":"requested migration onto first-class secrets; awaiting restart","fields":{"secret":"test/secrets/oauth-tokens"}}
{"message":"applying updated task specification"}
{"message":"resolved task secret","fields":{"secret":"test/secrets/oauth-tokens"}}
{"message":"resolved task secret","fields":{"secret":"test/secrets/rotate-every"}}
{"message":"resolved task secret","fields":{"secret":"test/connectors/registry.example/acme/source-secrets/oauth-client"}}
{"message":"connector applied"}
{"message":"resolved task secret","fields":{"secret":"test/secrets/rotate-every"}}
{"message":"resolved task secret","fields":{"secret":"test/secrets/oauth-tokens"}}
{"message":"resolved task secret","fields":{"secret":"test/connectors/registry.example/acme/source-secrets/oauth-client"}}
{"message":"opened","fields":{"source":"sibling","generation":0}}
```

The `resolved task secret` lines are the runtime's, one per stanza entry, and
the restart before them is the publication. The group appears **twice** because
the stanza is resolved on every connector start, and activating a build starts
the connector twice: once to `Apply` the new specification, and again for the
`Open` which begins the session. Within a group the three lines arrive in
whichever order their concurrent decryptions finish, which is why the two
groups above are not in the same order.

The session which requested the migration holds still until the restart — it
neither re-proposes it nor rotates, since the stanza it proposed names the
tokens it just stored. Confirm the model changed:

```bash
mkdir -p /tmp/migrated && mise exec -- flowctl catalog pull-specs --name test/secrets/source --target /tmp/migrated/flow.yaml
```

`pull-specs` writes into an existing directory and does not create one, so the
`mkdir` is not optional.

The `config` is now plaintext with an empty `credentials`, and the `secrets`
stanza has the three entries. Nothing published it but the connector.

### 5. Watch the rotation

Every `rotate_every` seconds — 30, from `test/secrets/rotate-every` — the
connector mints `gen-<n+1>`, stores it, and only then switches to it in memory.
The client id is the vendor's and does not rotate, so its digest holds:

```json
{"ts":"...:51Z","generation":1,"source":"sibling","client_id_digest":"d6b12258b72f","token_digest":"5ee4e1f377a3"}
```

```bash
mise exec -- flowctl secret list test/secrets/
```

`test/secrets/oauth-tokens` now exists, and its `secretId` changes with each
rotation — ids are time-ordered, so comparing two observations tells you which
is newer.

**`generation` is monotonic and never regresses**, restart boundaries included.
That is the second assertion, and it holds because the connector stores a
generation before it adopts one: a restarted session resolves what was stored,
which is never older than what the previous session reported. A store which
fails leaves memory where it was and is retried on the next tick.

### 6. The probe's verdict

```bash
mise exec -- flowctl collections read --collection test/secrets/probe --since 10m --follow
```

```json
{"ts":"...:21Z","generation":0,"source":"sibling","ok":true,"detail":null}
{"ts":"...:51Z","generation":1,"source":"sibling","ok":true,"detail":null}
```

`ok: true` is the derivation's own secret resolving: `ACTUAL_TOKEN` exists in
its environment only as the merged value of `test/secrets/token`, and
`EXPECTED_TOKEN` sits beside it in the clear.

Reads of a live data plane need the stack's CA on `SSL_CERT_FILE`; see
`mise run local:stack-info`.

The capture runs until you delete it, accumulating documents in `events` and as
many again in `probe`. Nothing prunes them:

```bash
mise exec -- flowctl catalog delete --prefix test/secrets/ --dangerous-auto-approve
```

### 7. Preview, where rotation is in memory only

```bash
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
  --name test/secrets/probe --fixture tests/secrets/events.fixture.jsonl
```

```json
["test/secrets/probe",{"ts":"2026-01-01T00:00:00Z","generation":0,"source":"legacy","ok":true,"detail":null}]
["test/secrets/probe",{"ts":"2026-01-01T00:00:01Z","generation":1,"source":"sibling","ok":true,"detail":null}]
```

Preview resolves secrets under the **logged-in user's** JWT, through
`/authorize/user/decrypt-secret` — a second authorization path into the same
resolution code, which the published task reaches via
`/authorize/task/decrypt-secret` instead.

Preview leaves the connector mount empty (`Service::new_local` holds no
data-plane key and has no control plane to rotate against), so there is no
`task-update.json` to read. To see the
connector say so rather than fail, preview the *capture* — fixtures are for
derivations, so this one runs the real image, pre-migration, and stops on
Ctrl-C:

```bash
mise exec -- flowctl preview --source tests/secrets/flow.yaml --name test/secrets/source --log-json
```

```json
{"message":"no task update context was injected; rotating in memory only. This is expected under `flowctl preview` and in tests."}
{"message":"rotated in memory only; no task update context was injected","fields":{"generation":1}}
```

`--log-json` is what makes those visible. Without it, task logs are routed
through `tracing`, whose default filter in flowctl is `WARN` — so every `INFO`
the connector writes is dropped, and the preview prints its documents and
nothing else. `--log-json` bypasses the filter and writes each task log to
stderr as JSON.

That is a supported mode, and it is why `rotation.py` distinguishes
`Unconfigured` from `RouteError`.

### 8. Rotate the probe's secret, and see the lazy pickup

A running task keeps the value it resolved at start; a new one picks up the
current secret. Rotate the token without updating `EXPECTED_TOKEN`:

```bash
echo -n "rotated-t0ken" | mise exec -- flowctl secret set test/secrets/token
```

```bash
mise exec -- flowctl preview --source tests/secrets/flow.yaml \
  --name test/secrets/probe --fixture tests/secrets/events.fixture.jsonl
```

```json
["test/secrets/probe",{"ts":"2026-01-01T00:00:00Z","generation":0,"source":"legacy","ok":false,
  "detail":"actual(len=13 sha256=84125a2bdc7a) != expected(len=12 sha256=fe5f98c6100c)"}]
```

Restore the original value to go green again.

### 9. Delete, and see the dangling reference

Publication does not check that a referenced secret exists; resolution does,
and names both the secret and the configuration location it serves. The delete
itself succeeds — the failure shows on the next start of anything which names
the secret, such as the preview of step 8 run again:

```bash
mise exec -- flowctl secret delete test/secrets/token
```

```
resolving `secrets` stanza: failed to resolve secret 'test/secrets/token', used at configuration location /environment/ACTUAL_TOKEN: control-plane API responded 404: secret 'test/secrets/token' does not exist
```

The published `test/secrets/probe` fails the same way at its next restart, so
put the secret back before leaving the stack up:

```bash
echo -n "s3cr3t-t0ken" | mise exec -- flowctl secret set test/secrets/token
```

## The guards

Each of these is rejected before the task is published. Copy `flow.yaml` aside
and edit it to see them. All but one are decided from the model alone, before
any connector runs; the plaintext check is the exception, because it needs the
`config_schema` of the connector's `Spec` response and so rejects only after
the connector has started and answered.

| Edit | Rejected by | Error |
| --- | --- | --- |
| `enable-runtime-v2: "false"` on the derivation | publication | uses `secrets`, which requires the V2 task runtime |
| secret `test/other/token` on the derivation | publication | may use only secrets which are its siblings |
| a top-level `sops` key beside a stanza | publication | is either wrapped as a whole by `sops`, or is plaintext |
| a plaintext `credentials.client_id` beside a stanza | publication | is annotated `secret: true` and cannot hold a plaintext value |
| an image-rule secret on the **derivation** | publication | may use only secrets which are its siblings (a built-in derivation has no image identity, so it gets no image rule) |
| an image-rule secret the image doesn't declare | the **reactor**, before `docker run` | neither a sibling of the task nor declared by image ... |
| the image-rule secret at `/host` rather than `/credentials` | the **reactor**, before `docker run` | may only be merged where its image declares |
| `dev.estuary.secrets` naming another image's secret | the **reactor**, at start | the image rule cannot admit it |

The split matters. Publication's copy of the image rule is fail-fast ergonomics
only: it can see the task's image, but not what that image declares. The
reactor sees both, and is the real enforcement point — as is the control plane,
which will refuse a decryption the attested image cannot justify.

## What this catalog does not cover

- **Materializations.** The resolution path is shared by all three task types
  (`crates/connector/src/protocol.rs`), and the per-type validation checks are
  covered by `crates/validation/tests/secrets_tests.rs`. This catalog exercises
  a capture and a derivation.
- **A hostile connector.** Nothing here tries to set a secret it doesn't own,
  or to update a configuration under a stale build. Those denials are covered
  by the route tests in
  `crates/control-plane-api/src/server/task_update.rs`.
- **The route failure path.** Everything above is the success path. A local
  stack reaches the two routes from inside a container because the reactor
  adds their exact `*.flow.localhost` names to the container through Docker's
  `host-gateway`. To watch the designed fallback instead — a `WARN` naming the
  status and body, the new credential kept in memory, and a retry on the next
  tick — temporarily remove those mappings in `crates/connector/src/image.rs`
  and restart the reactor.
