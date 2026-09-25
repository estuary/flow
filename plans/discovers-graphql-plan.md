# Discovers GraphQL API

Status: Draft

Date: 2026-09-25

This plan adds `createDiscover` and `discover(id)` to the GraphQL API in `control-plane-api`, with fields for status, errors, and logs. A discover is an asynchronous job that runs discovery on a capture definition and merges the resulting capture and collection definitions into a draft. It does not publish the draft.

The mutation uses the capture definition in the draft if one exists. Otherwise, it copies the live capture of that name into the draft, provided the caller can read it. If neither definition is available, the mutation fails. One transaction writes the copied definition, if needed, and inserts the discover row. The existing database trigger creates the executor's task in that transaction. Clients poll for the result.

The `discovers` table and its row-level security policies do not change. Existing PostgREST clients can continue to use them.

## What this builds on

Discovery asks the connector for potential bindings. The executor uses the discovered bindings to update the drafted capture's bindings and create or update target collection in the draft. It records the job's outcome in `discovers`.

The new API uses this existing process:

- The executor prepares a capture from the draft, a readable live capture, or an initial definition that it constructs. It replaces the endpoint's image and configuration with values from the discover row. It also takes `update_only` from that row. GraphQL requires a definition at submission and copies it into the draft when needed. PostgREST callers retain all three preparation paths.
- The capture model carries secret references and the redaction salt. This API adds no separate inputs for them and does not change secret resolution.
- Before running the connector, the executor checks the connector tag, `SpecEdit` on the capture name, and access to the data plane. Access to the plane requires legacy `read`, which includes every Viewer capability. The executor also needs the plane's first configured HMAC key to sign requests to its connector proxy.
- A discover belongs to its draft's owner. The table has no user column, and deleting a draft deletes its discovers. Draft ownership therefore controls reads of a discover.
- The log writer associates connector logs with the row's `logs_token`. `Discover.logs` uses that token internally.

## Capture configuration and discovery

`CaptureDef` combines two concerns today. Its endpoint, bindings, and runtime settings describe the capture itself. Its `autoDiscover` settings tell the control plane how discovery should modify that capture and its target collections.

The name `autoDiscover` makes the second concern sound exclusive to automatic discovery. The field's presence enables periodic discovery, but its flags also affect manually requested discovery. For example, `flowctl discover` derives `update_only` from `addNewBindings`, and the discover executor uses `evolveIncompatibleCollections` when collection keys change. The field therefore combines whether discovery runs automatically with policy for applying discovery results.

The [policy discussion](https://estuaryworkspace.slack.com/archives/C03QBN83GQ4/p1790200476989949) considered letting manual discovery use different settings from the capture's ongoing policy. A caller might want to enable new bindings during one manual discover while leaving automatic addition disabled. We chose to keep discovery policy on the capture and have discovers use it. This follows the broader decision to treat [discovery as an operation applied to a capture](https://estuaryworkspace.slack.com/archives/C03QBN83GQ4/p1790202935042549). It avoids a second set of endpoint, secret, and policy inputs that would duplicate parts of `CaptureDef` on each discover request.

This API therefore uses the selected capture's configuration and `autoDiscover` settings, with the defaults described below. The mutation accepts no separate configuration arguments or policy overrides. Clients that want a different discovery policy must change it in the draft's capture definition.

Renaming or aliasing `autoDiscover` to `discovery` would express this broader role: policy for discovery, whether initiated manually or automatically. The policy would remain part of `CaptureDef`. This API adopts that understanding while retaining the current field name and defaults. The rename or alias can follow separately.

Here, a new capture means one with no live definition, even if the draft already contains a definition.

`createDiscover` takes an existing draft, a capture name, and an optional data plane for this operation. It selects the definition as follows:

1. If an entry exists under that name in the draft, it must contain a capture model that deserializes as `CaptureDef`. A deletion, another catalog type, or a model that fails to deserialize is an error.
2. If no entry exists under that name, the mutation uses a readable live capture of the same name. Other entries in the draft do not prevent the mutation from using the live capture.
3. If neither definition is available, the mutation returns an error.

For a new capture, the client stages an initial definition with `stageDraftSpecs` before requesting discovery. The definition must include `bindings`, which may be `[]`. For an existing capture, the client can stage edits or let the mutation copy the live definition. Once the mutation commits, the copied definition is visible in the draft even if discovery later fails.

`createDiscover` derives `connector_tag_id`, `endpoint_config`, and `update_only` from the selected definition. The existing executor needs these columns. PostgREST clients continue to supply them directly. This API adds no columns for capture fields and does not store a complete copy of the job's inputs. Clients read the current capture and collection definitions through the draft API.

### Draft contents after submission

`Discover` describes the job. Clients read the current definitions through the `specs` connection on `draft(id)`.

When the mutation copies a live capture, it stores the capture's last publication ID as `expectPubId`. A later publication can then detect intervening changes to the live capture. Submission leaves existing draft entries and their publication preconditions unchanged.

The discover row fixes the endpoint and `update_only` at submission. The executor uses these values even if the client subsequently edits the draft. It reads the other fields, including `autoDiscover.evolveIncompatibleCollections` and secret references, when it loads the draft. A successful discover writes its merged definitions into the draft, replacing any intervening endpoint edits.

Copying the live definition during submission gives the executor that definition as its base, provided the draft entry remains unchanged until execution. Clients can still edit or unstage it. The executor retains its existing behavior if it later finds no capture in the draft, including constructing an initial definition when necessary.

The draft is not a historical record of the inputs to a discover. Concurrent edits and jobs can overwrite each other's changes. This API does not serialize those operations. Clients should wait for a discover to finish before editing its draft or starting another operation on it.

### Client migration

Existing PostgREST clients can still request discovery without a draft entry or a live capture. The executor constructs an initial capture definition from the discover row. GraphQL clients must stage a new capture first. The UI migration must add this step wherever the UI currently relies on the executor to construct the capture.

The UI must supply its intended `autoDiscover` settings in that definition. The executor currently sets both flags to `true` when it constructs a capture. Clients that need those defaults must set them explicitly. Staging only an endpoint and empty bindings does not reproduce those settings.

When the executor constructs a capture, it also sets the draft entry's `expectPubId` to zero. Publication therefore fails if someone creates a live capture of that name in the meantime. To retain that protection, the client must set `expectPubId` to zero (`0000000000000000`) each time it stages the new capture. Omitting this field or passing null clears any previous precondition.

When a user edits a capture and re-enables disabled capture bindings, the UI currently forces `update_only` for that discover. Through this API, the capture's `autoDiscover` settings determine the behavior instead. The merge adds new capture bindings with `disable: false` unless the policy or connector requires `disable: true`. These changes take effect only after publication.

The UI reads discover logs through PostgREST today. It must use `Discover.logs` or continue to fetch `logs_token` from the discover row through PostgREST. The GraphQL response does not expose that token.

## Schema

```graphql
extend type QueryRoot {
  """
  The query returns null if the discover does not exist or the caller does not own its draft.
  """
  discover(id: Id!): Discover
}

extend type MutationRoot {
  """
  Discovery uses the capture in the draft if present.
  Otherwise, the mutation copies a readable live capture of the same name into the draft.
  The mutation fails if neither is available.
  It also fails if the draft entry is a deletion or cannot be used as a capture.

  Discovery uses the connector's discovered bindings to update the capture's bindings and target collection definitions in the draft.
  Read the resulting definitions through the specs connection on draft(id).
  """
  createDiscover(
    draftId: Id!
    captureName: Name!

    """
    This argument selects the data plane for discovery, not for publication.
    Omission and null have the same meaning.
    A live capture uses its current plane. A supplied plane must match it.
    A capture with no live definition uses the first plane in its storage mapping unless this argument selects another permitted plane.
    Submission fails if the caller cannot use the selected plane.
    """
    dataPlane: String
  ): Discover!
}

type Discover {
  id: Id!
  draftId: Id!
  captureName: Name!
  dataPlaneName: String!
  status: DiscoverStatus!
  createdAt: DateTime!
  updatedAt: DateTime!

  """
  These are the draft's current errors, which all jobs on the draft share.
  They may predate this discover or concern other specifications.
  Later jobs can replace them.
  """
  errors: [Error!]!

  """
  Pagination follows timestamp order and can miss lines from delayed writes.
  Cleanup deletes lines older than two days.
  More lines can arrive after discovery finishes.
  """
  logs(after: String, first: Int): LogLineConnection!
}

enum DiscoverStatus {
  "The discover is queued or in progress."
  queued
  success
  wrongProtocol
  tagFailed
  imageForbidden
  discoverFailed
  noDataPlane
  notAuthorized
  "This status occurs only on historical discovers."
  mergeFailed
  "This status occurs only on historical discovers."
  deprecatedBackground
  "This status occurs only on historical discovers."
  pullFailed
}

type LogLineConnection {
  pageInfo: PageInfo!
  edges: [LogLineEdge!]!
}

type LogLineEdge {
  cursor: String!
  node: LogLine!
}

type LogLine {
  loggedAt: DateTime!
  stream: String!
  line: String!
}
```

`Id`, `Name`, `DateTime`, `Error`, and `PageInfo` reuse existing types. `Error` is the type that `Draft.errors` uses. Log cursors use the existing `TimestampCursor` representation of `loggedAt`.

## createDiscover

All operations require authentication. `createDiscover` checks draft ownership before inspecting the capture. A draft owned by someone else gives the same "draft not found" error as a missing draft, matching the draft API. The mutation accepts or rejects the request in one transaction.

The mutation rejects the request without committing any changes in these cases:

- The caller lacks `SpecEdit` on `captureName`.
- The draft entry is a staged deletion, has `delete: true`, names another catalog type, or cannot deserialize as `CaptureDef`. A live capture of the same name does not replace an invalid draft entry.
- The draft has no entry under `captureName`, and the caller cannot read a live capture of that name with `CatalogRead`. Missing and unreadable live captures give the same error.
- The endpoint does not identify a connector image, or its configuration is not an inline JSON object. The `discovers.endpoint_config` column requires an object, even though `CaptureDef` also accepts references to configuration files.
- The image reference does not identify a known connector tag with protocol `capture` and job status `success`.
- No data plane satisfies the selection and authorization rules below. Missing and unauthorized planes give the same error.

On acceptance, the transaction inserts the discover row and any required capture definition. The existing trigger schedules discovery. The mutation returns the job with status `queued`:

- If the draft has no entry under `captureName`, the mutation copies the full live definition and updates the draft's modification time. It preserves the serialized definition and sets `expectPubId` to the live capture's last publication ID. It must not replace an entry that another request stages while submission is in progress. A conflicting insert can reject the request so the client can retry.
- Existing draft entries do not change. The mutation does not copy associated live collections. The executor merges collections when discovery runs.
- If the capture is already in the draft, submission leaves the draft's modification time unchanged.
- The row records `connector_tag_id`, `endpoint_config`, and `update_only` as described below.
- Existing draft errors remain until the executor applies its outcome.

An exact copy reports `isUnchanged: true` while it still matches the live definition and the reader has `CatalogRead`. A later live publication, draft edit, or permission change can make that field false.

`connector_tag_id` identifies the `connector_tags` row for the endpoint's image name and tag or digest. A connector tag job requests `Spec`, validates the returned metadata, and updates that row. `endpoint_config` contains the endpoint configuration from the capture definition. Staging and submission must preserve the order and values of encrypted configuration fields so SOPS can verify the document. Copying a live definition must preserve its JSON text as well, because `isUnchanged` compares serialized definitions.

A rejection leaves the draft, its errors, its modification time, and the job queue unchanged. Acceptance does not validate endpoint credentials or guarantee that the connector can connect to the external endpoint. The executor rechecks the connector tag, `SpecEdit`, and access to the selected plane when it runs. It also needs the plane's first configured HMAC key to authenticate to the connector proxy. Changes to permissions, connector metadata, or the selected plane can therefore cause a later failure.

The executor uses the plane named in the discover row. It does not repeat selection against the storage mapping.

Connector and merge failures leave any definition copied during submission in the draft. An unrelated malformed definition in the draft can also cause discovery to fail. The executor loads the whole draft, and any errors in that loaded draft prevent it from committing the merged definitions.

### Discovery policy defaults

The API derives `update_only` from `autoDiscover`, following `flowctl discover`. The executor derives the policy for changed keys from the capture it loads:

| `autoDiscover` | `update_only` | Mark collections for reset when their keys change |
| --- | --- | --- |
| Missing or null | `false` | No |
| `{}` | `true` | No |
| Explicit flags | `!addNewBindings` | `evolveIncompatibleCollections` |

When `update_only` is `true`, new capture bindings enter the draft disabled. When it is `false`, the connector can still recommend disabling a new binding. This flag does not prevent discovery from removing capture bindings whose resource paths are absent from the discovered bindings.

The table describes how this API derives policy. PostgREST clients continue to supply `update_only` directly. Missing or null `autoDiscover` also disables periodic automatic discovery. The API does not insert the object, change its flags, or accept overrides for an individual job.

### Data plane

The optional `dataPlane` argument selects the plane for this discovery operation. Omission and null have the same meaning:

- An existing live capture uses its current plane, even when the draft contains an edited definition. This also applies when the caller lacks `CatalogRead` on the live capture. That permission controls copying the definition, not selecting the plane. A supplied `dataPlane` must name the current plane.
- A new capture uses a supplied `dataPlane` if publication's rules for new specifications permit it under the applicable storage mapping.
- Without an explicit selection, a new capture uses the mapping's primary plane: the first plane in its list.

The [earlier discussion](https://estuaryworkspace.slack.com/archives/C03QBN83GQ4/p1790204987260919) allowed an explicit selection from the storage mapping for either a new or an existing capture. This plan proposes a narrower rule for existing captures. Named secrets permit decryption only from the capture's current plane. An endpoint may also restrict connections to that plane's addresses. The proposed rule rejects a different plane at submission instead of accepting a job that can fail for these reasons.

Publication ignores an explicit plane for an existing capture. Discovery instead rejects a different plane, so it does not silently substitute one the client did not request.

For a new capture, submission fails if no storage mapping applies or the mapping does not permit the supplied plane. If selection needs a primary plane, the mapping must list one. Reuse publication's placement rules, including its exception for the `ops/` mapping, rather than implementing a separate membership check.

At submission, every selected plane must pass the executor's checks for authorization and a usable signing key. This includes the current plane of an existing capture.

The mutation stores the selected plane in `discovers.data_plane_name` and returns it as `Discover.dataPlaneName`. This does not set the plane for publication. A later publication independently selects the plane for a new capture. Clients that require the same plane must also select it when publishing.

## discover(id)

For an authenticated caller, `discover(id)` returns the row if the caller owns its draft, and `null` otherwise. It reads rows created through either GraphQL or PostgREST.

`status` reports progress or the outcome. `queued` includes both waiting and running. `success` means that discovery merged its results into the draft, not that the draft passed publication validation. `errors` returns the draft's current errors, which can change independently of this discover's status.

Staging definitions leaves existing errors in place. Each discover or publication replaces them when it applies its outcome. Some discover failures clear the errors without inserting new ones: `noDataPlane`, `tagFailed`, `wrongProtocol`, and `imageForbidden` report only the status.

The enum retains all existing status values, including `mergeFailed`, `deprecatedBackground`, and `pullFailed`, so historical rows remain readable. The schema marks those values as historical outcomes. `Discover.status` exposes only the discriminator from `job_status`. It does not expose additional properties such as a historical `publication_id`. The stored JSON remains unchanged for PostgREST readers.

## Logs

### Ordering

The agent's [log writer](https://github.com/estuary/flow/blob/93f99e1f780406796bbc9dcaa02918c91201fc44/crates/control-plane-api/src/logs.rs#L66-L146) currently gives all lines in a batch the same timestamp. The table has no other ordering field. A cursor containing only a timestamp cannot resume from the middle of a batch.

Change this writer to assign timestamps at PostgreSQL's microsecond precision. Each timestamp must be at least one microsecond later than the previous timestamp from that writer, including across batches. It must also be no earlier than the writer's current clock reading at that precision. The writer must retain the previous timestamp between batches, even if its clock moves backward.

The data-plane controller's writer already increments timestamps by one microsecond [within each batch](https://github.com/estuary/flow/blob/93f99e1f780406796bbc9dcaa02918c91201fc44/crates/data-plane-controller/src/shared/logs.rs#L103-L105). The proposed change also orders separate batches from the same writer. It requires no table migration.

This changes logs for every operation that uses the agent's writer, including publications, connector tag jobs, and validation. It provides order within one writer's lifetime. A restarted writer or another agent can produce duplicate or earlier timestamps for the same discover, because writers do not coordinate clocks or commits.

Timestamps will come from the agent's clock instead of the database's. Differences between those clocks can affect both pagination across writers and retention. Cleanup compares `logged_at` with the database clock and deletes lines older than two days. Small clock differences only shift that retention window slightly, but the API cannot assume all clock differences are small.

Existing lines retain their shared timestamps until cleanup deletes them. Agents that still run the old writer during deployment can also produce such lines. A page boundary within one of these groups can skip lines.

### Reading

`Discover.logs` returns lines in ascending `loggedAt` order. It uses the existing `TimestampCursor`, with the last returned line's timestamp as the next cursor. Subsequent pages select timestamps strictly greater than that cursor. Invalid cursors and negative page sizes return errors. The default page size is 100, with no maximum, matching the draft connections.

A delayed write from an earlier attempt can have a timestamp at or before a cursor the client already received. Subsequent pages will miss those lines. Duplicate timestamps from separate writers have the same problem. Pagination therefore remains best effort, even after the writer change.

The existing index on `token` can locate a discover's logs. The query must then sort them by timestamp. This plan adds no index or migration.

Lines reach the table asynchronously, and some can arrive after the status leaves `queued`. There is no signal that all logs are available. `hasNextPage: false` means the query found no additional visible lines beyond the returned page. A client can continue polling after discovery finishes, but an empty page does not prove that no more lines will arrive.

A completion guarantee would require the log writer to acknowledge committed lines before the executor records the final status. No such coordination exists today.

`LogLine` and its connection can later support publication logs as well. Resolving logs through their parent discover keeps `logs_token` out of the GraphQL API. Each log query must enforce draft ownership, as the draft's nested resolvers do.

## Authorization

The mutation checks `SpecEdit` with the caller's token. Copying a live capture also requires `CatalogRead` with that token. The mutation treats an unreadable live capture as absent and does not copy its definition. The executor makes its own checks with the user's full grants.

Two existing authorization limitations remain:

- The executor never receives the token or its restrictions. It can copy existing collection definitions into the draft using the user's full `CatalogRead` permission. A restricted token can then read those definitions through the draft, even if it cannot read them directly from the live catalog. Draft ownership alone controls access to staged contents. Enforcing token restrictions through execution requires coordinated changes to drafts, discovers, and publications.
- Legacy `read` on the data plane requires every Viewer capability. The Editor bundle lacks `ViewDataPlanePrivateNetworking`. A user authorized on the plane only through Editor therefore fails this check. So does a token restricted to Editor capabilities. The API applies the same check at submission to detect this failure before scheduling the job. Publications also require legacy `read` on the plane.

Reading a discover, its errors, and its logs requires ownership of its draft. Each resolver must enforce this ownership requirement.

## Orphaned discover tasks

Deleting a draft deletes its `discovers` rows, but their executor tasks remain. The executor then [fails to load the row](https://github.com/estuary/flow/blob/93f99e1f780406796bbc9dcaa02918c91201fc44/crates/control-plane-api/src/discovers/db.rs#L30-L59). The automation server logs a warning and retries after the task's heartbeat expires. This repeats without limit today, including after `deleteDraft`.

Change the executor to finish the task when its discover row no longer exists. A missing row is the only error that this change treats as completion. Database failures must still propagate and allow a retry.

A draft can also disappear while its discover runs. The executor cannot apply results to the deleted draft, so that attempt fails. The next attempt finds no discover row and finishes the task.

This change does not cancel a running connector. The task continues to receive heartbeats while execution is in progress. Cleanup therefore has no fixed deadline after draft deletion. It depends on the current attempt finishing or failing and the scheduler running another attempt after the heartbeat expires.

The discover row does not need to outlive its draft. Its merged definitions and errors belong to the draft. The table therefore needs no separate user column for this API.

## Delivery

The work lands as two independent PRs: the API, including log ordering, and the executor fix for tasks whose discover rows no longer exist.

Submission tests check the persisted draft, discover row, and scheduled task as well as the response. They cover:

- Unauthenticated requests.
- A missing draft and a draft owned by someone else return the same error.
- A staged capture takes precedence over a different live definition. Submission preserves its model, metadata, unrelated entries, and the draft's modification time.
- Copying a live capture succeeds in an empty draft and in a draft containing unrelated definitions. The copied definition is visible before execution and retains the live capture's last publication ID as `expectPubId`. It reports `isUnchanged: true` to an authorized reader while the live definition still matches. Only the new entry and the draft's modification time change.
- A concurrent insertion under the capture's name prevents the mutation from overwriting that entry with the live definition.
- The mutation rejects a capture absent from both the draft and readable live definitions. It creates neither an initial definition nor a job.
- The mutation rejects staged deletions, `delete: true`, other catalog types, and malformed capture models. A live capture of the same name does not bypass these errors.
- The mutation rejects endpoints without a connector image and configurations that are not inline JSON objects, including file references.
- The mutation rejects a caller without `SpecEdit`. Copying a live definition also requires `CatalogRead`. Missing and unreadable live captures give the same error. A valid staged capture needs no `CatalogRead` on the live definition, but discovery still uses the live capture's plane.
- Omission and null select the current plane for an existing capture and the mapping's primary plane for a new capture. An explicit permitted plane works for a new capture. For an existing capture, only an explicit selection of its current plane succeeds, including when the draft contains an edited definition. The stored and returned plane names match.
- Selection rejects missing mappings, mappings without a required primary plane, disallowed planes, and planes without usable signing keys. Missing and unauthorized planes give the same error. The tests also cover publication's exception for the `ops/` storage mapping.
- The mutation rejects unregistered image references, tags for other protocols, and tags whose job did not succeed.
- Missing, null, empty, and explicitly configured `autoDiscover` produce the documented policy. Submission preserves the capture's settings and stores the expected `update_only`.
- Encrypted endpoint configurations still pass SOPS verification after submission, whether the client staged the capture or the mutation copied it from live.
- Rejections leave the draft, errors, modification time, and job queue unchanged, including failures after writes begin. Successful submissions retain existing draft errors.

Query tests cover ownership, missing rows, historical statuses, and the current errors after a failed job. They also check that a later job can replace the errors without changing the earlier discover's status. Logs must not disclose another draft's lines.

Log tests cover page boundaries within and between batches, empty pages, invalid cursors, and negative page sizes. They check timestamp order after PostgreSQL stores and returns the values. They also cover a backward clock adjustment between batches and logs that arrive after discovery finishes.

Agent integration tests run discovers submitted through GraphQL using both staged and copied capture definitions. They cover:

- Updated capture bindings and target collection definitions, including new disabled bindings, removed bindings, and collection definitions marked for reset under the selected policy.
- A connector failure that leaves the copied capture in the draft and records the failure.
- An unrelated malformed draft entry that prevents the executor from committing merged definitions and produces draft errors.
- A later publication that uses the copied `expectPubId` and rejects an intervening live change.

The executor fix has separate tests for deletion before execution and deletion during execution. The latter lets the running attempt finish, then checks that the next attempt completes the task. Database failures must still cause retries.

## Deferred work

The API leaves these changes to separate work:

- Rename or alias `autoDiscover` to `discovery`, preserving the distinction between an absent field and an empty object.
- Revisit the permission required on a data plane, including the effect on the Editor bundle. This API keeps the executor's current authorization rule.
- Coordinate concurrent edits and jobs on the same draft.
- Enforce token restrictions on definitions that executors copy into drafts.
