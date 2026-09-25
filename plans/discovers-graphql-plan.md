# Discovers GraphQL API

Status: Draft

Date: 2026-09-24

This plan adds a GraphQL API for discovers to `control-plane-api`: a `createDiscover` mutation, a `discover(id)` query, and the discover's errors and logs. Discovery operates on a capture definition and merges the results into a draft.

The mutation uses the drafted capture if present. Otherwise, it copies a readable live capture into the draft. If neither exists, the mutation fails. The mutation commits any live-capture copy and the discover row in one transaction. The existing executor runs the discover, and clients poll for the result. The table and its RLS policies don't change, so existing PostgREST clients keep working.

## What this builds on

Discovery asks a connector for available resources, then merges the discovered bindings and collection definitions into a draft. The current asynchronous path records a job in `discovers`. A trigger queues a task for each new row. The executor ([`agent/src/discovers.rs`](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/agent/src/discovers.rs)) checks the request, runs discovery, and writes the outcome. These parts of that design shape the API:

- The executor prepares a capture from the draft, a readable live capture, or a starter definition. It replaces the endpoint's image and configuration with the values from the discover row, and takes `update_only` from that row ([L225-236](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/agent/src/discovers.rs#L225-L236), [L331-375](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/agent/src/discovers.rs#L331-L375)). GraphQL submissions ensure that a capture is staged when the mutation commits. The executor's existing preparation paths remain available to PostgREST callers.
- The complete capture model carries any secret references. This API adds no separate secret inputs or changes to secret resolution.
- Before running the connector, the executor checks the connector tag, `SpecEdit` on the capture name ([L172-202](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/agent/src/discovers.rs#L172-L202)), and legacy `read` on the data plane, which requires every Viewer bit ([L204-223](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/agent/src/discovers.rs#L204-L223)).
- A discover belongs to its draft's owner: the table has no user column, and deleting a draft deletes its discovers. Reads of a discover are therefore authorized by draft ownership.
- The connector's logs go to `internal.log_lines` under the row's `logs_token`, which `Discover.logs` reads.

## Capture configuration and discovery

`CaptureDef` carries the capture configuration and discovery policy. Manual and automatic discovery use its `autoDiscover` settings. The mutation has no separate configuration arguments or policy overrides. Renaming or aliasing `autoDiscover` to `discovery` is separate work.

`createDiscover` takes an existing draft, a capture name, and an optional data plane for this operation. It selects the definition as follows:

1. If an entry exists under that name in the draft, it must contain a valid capture model. A deletion, another catalog type, or an invalid capture model is an error.
2. If no entry exists under that name, the mutation uses a readable live capture of the same name. Unrelated specifications in the draft do not prevent this fallback.
3. If neither definition is available, the mutation returns an error.

For a new capture, the client stages an initial definition with `stageDraftSpecs` before requesting discovery. The definition can have empty bindings. For an existing capture, the client can stage edits or let the mutation copy the live definition. The copy and the discover row commit together. Once the mutation succeeds, the copied capture is visible in the draft even if discovery later fails.

The resolver derives `connector_tag_id`, `endpoint_config`, and `update_only` from the selected definition. These columns adapt the request to the existing executor. PostgREST clients continue to supply them directly. This API adds no capture-shaped columns or historical input snapshots. Clients read the current capture and collection definitions through the draft API.

This follows the [capture-selection decision](https://estuaryworkspace.slack.com/archives/C03QBN83GQ4/p1790202935042549) and the [shared discovery-policy decision](https://estuaryworkspace.slack.com/archives/C03QBN83GQ4/p1790200476989949).

Consequences:

- `Discover` describes the operation and doesn't return a capture spec. Clients read the current specs with `draft(id) { specs }`.
- The live copy retains the source's last publication ID as `expectPubId`. A later publication can therefore detect intervening changes to the live capture. Submission leaves existing staged entries and their publication preconditions unchanged.
- The endpoint and `update_only` are fixed when the row is written. The rest of the drafted spec, including `autoDiscover.evolveIncompatibleCollections`, is read when the executor loads the draft to run the discover. This is existing behavior.
- Concurrent edits or operations on the same draft can overwrite one another's changes. This risk exists today and isn't addressed by this API.
- Copying the live definition during submission gives the executor that definition as its base instead of a later live version. It does not freeze the draft or guarantee a historical record of the inputs used by the executor.
- The three columns stay while callers and the executor depend on them. This API does not remove the executor's existing fallback or endpoint-replacement behavior.

### Client migration

Existing PostgREST clients can still request discovery without a drafted or live capture. The executor constructs their starter definition from the row. GraphQL clients must stage a new capture first. The UI migration must include this step where it currently relies on the starter-definition behavior.

The UI must supply its intended `autoDiscover` settings in that definition. The executor's current starter sets both flags to `true` ([L353-368](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/agent/src/discovers.rs#L353-L368)). Clients that need those defaults must set them explicitly. Staging only an endpoint and empty bindings does not reproduce those settings.

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
  Otherwise, the mutation copies the live capture with the same name into the draft.
  The mutation fails if neither exists or the draft marks the capture for deletion.

  Discovery merges the discovered bindings and collection definitions into
  the draft. Read the resulting specifications through draft(id) { specs }.
  """
  createDiscover(
    draftId: Id!
    captureName: Name!

    """
    When supplied, discovery uses this plane if the storage mapping permits it.
    Otherwise, discovery uses the live capture's plane, or the mapping's primary plane for a new capture.
    This choice applies only to discovery.
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
  Operations share the draft's current errors.
  These errors may predate this discover or concern other specifications.
  They do not provide a historical record of this discover.
  """
  errors: [Error!]!

  """
  Pagination follows timestamp order and is best effort.
  Logs expire after two days.
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

`Id`, `Name`, `DateTime`, `Error`, and `PageInfo` reuse existing types. `Error` is the type that `Draft.errors` uses. `LogLineConnection` uses `loggedAt` as its cursor.

## createDiscover

`createDiscover` runs these steps in one transaction:

1. Checks that the caller owns the draft. A draft owned by someone else gives the same "draft not found" error as a missing one, as in the draft API.
2. Checks `SpecEdit` on `captureName`, using the caller's token.
3. Selects the capture definition. An existing draft entry must have the capture type and a model that parses as `models::CaptureDef`. A null model, `delete: true`, another catalog type, or an invalid capture model is an error. Only the absence of an entry permits live fallback. The fallback requires `CatalogRead` on the live capture using the same token. A missing or unreadable live capture gives the same error. The selected endpoint must be a connector image.
4. Chooses the data plane (below), and checks it the same way the executor does. A missing plane and an unauthorized one give the same error.
5. Resolves the connector tag from the endpoint image. `models::split_image_tag` splits the image into its name and tag, and a query on `connectors` and `connector_tags` returns the tag's ID, protocol, and spec status. The tag must exist, must be a capture, and its spec must have succeeded. `connector_tags::fetch_connector_spec` makes the same lookup for publications but doesn't return the tag's ID, so this is a sibling query.
6. If the definition came from live specs, inserts that capture into the draft with `expect_pub_id` set to its `last_pub_id`. The mutation copies the full definition and updates the draft's modification time. It does not replace existing staged entries or copy associated live collections at submission. The executor merges collections when discovery runs.
7. Inserts the `discovers` row and commits the transaction. The existing trigger queues the task. The mutation returns the discover as queued.

Any submission error rolls back the live copy, draft timestamp change, discover row, and queued task together. A later connector or merge failure leaves the submitted copy in the draft. Submission does not clear existing draft errors. The executor replaces them when it applies the outcome, as it does today.

The server fills `connector_tag_id`, `endpoint_config`, and `update_only` as follows:

- `connector_tag_id` is the ID found in step 5.
- `endpoint_config` is the selected definition's connector `config`. The connector config is a `models::RawValue`. JSON member order is preserved, which sops needs to verify an encrypted config. The same requirement applies when copying a live definition into the draft.
- `update_only` is `!autoDiscover.addNewBindings`, and `false` when `autoDiscover` is missing or null. This matches [`flowctl discover`](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/flowctl/src/discover/mod.rs#L104-L108).

When the discover runs, the executor writes the image and config from these columns back into the drafted spec. `createDiscover` copied them from that spec, so the write-back changes the spec only if its endpoint was edited after the discover was submitted.

The mutation checks the capture, connector, and plane before accepting the job. The executor retains its existing checks. A successful mutation means the job was accepted. It does not establish that the endpoint configuration works. Connector and merge failures are asynchronous discover outcomes.

### Discovery policy defaults

The API preserves the existing manual-discovery defaults:

| `autoDiscover` | `update_only` | Mark collections for reset when their keys change |
| --- | --- | --- |
| Missing or null | `false` | No |
| `{}` | `true` | No |
| Explicit flags | `!addNewBindings` | `evolveIncompatibleCollections` |

When `update_only` is `true`, new bindings enter the draft disabled. When it is `false`, the connector can still recommend that a new binding be disabled. Missing or null `autoDiscover` also disables periodic automatic discovery. The API does not add the stanza, change its flags, or accept separate per-run overrides.

### Data plane

The optional `dataPlane` argument selects the plane for this discovery operation. Omission and null have the same meaning. Selection follows the [agreed discovery-specific rules](https://estuaryworkspace.slack.com/archives/C03QBN83GQ4/p1790204987260919):

- If `dataPlane` is supplied, use it after checking that the storage mapping permits it. This applies to both new and existing captures.
- Otherwise, an existing live capture uses its current plane, even when its definition is staged in the draft.
- Otherwise, a new capture uses the primary plane, which is the first plane listed in its storage mapping.

Reuse validation's storage-mapping lookup and explicit-plane checks, exposing the necessary helper rather than duplicating the rules. Preserve the existing `ops/` exception to explicit-plane membership checks ([`validation/src/lib.rs` L349-404](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/validation/src/lib.rs#L349-L404)). Do not apply publication's rule that keeps an existing task on its current plane when an explicit discovery plane was requested. Every selected plane must pass the executor's authorization and availability checks. If selection needs a primary plane and the mapping has none, submission fails. An invalid explicit selection also fails submission.

The resolver writes the selected plane to `discovers.data_plane_name`. `Discover.dataPlaneName` reports that selection. This choice does not move a live capture or set the plane for publication. A later publication makes its own placement decision.

## discover(id)

`discover(id)` returns the row if the caller owns its draft, and `null` otherwise. It reads rows created through either GraphQL or PostgREST.

`status` reports progress or the outcome. `queued` includes both waiting and running. `errors` reads the current `draft_errors`, with the shared and mutable semantics described in the schema.

Staging new specs leaves these errors in place. Each discover or publication replaces them when it applies its outcome. Some discover failures record no errors: `noDataPlane`, `tagFailed`, `wrongProtocol`, and `imageForbidden` report only the status.

`DiscoverStatus` values are camelCase so that they equal the stored `job_status.type` strings, as with the publication `StatusType`. Enums defined only for GraphQL use SCREAMING_CASE. The executor's `JobStatus` moves from `agent` to `models::discovers` so that `control-plane-api` can expose its status. The stored JSON doesn't change.

All existing variants are retained, including `mergeFailed`, `deprecatedBackground`, and `pullFailed`, so historical rows remain readable. Their GraphQL descriptions mark them as historical outcomes. The success fields `publication_id` and `specs_unchanged` keep their existing serialization and aren't exposed by `Discover`.

## Logs

### Ordering

The agent's log writer ([`control_plane_api::logs::serve_sink`](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/control-plane-api/src/logs.rs#L66-L146)) inserts each batch of lines in one statement with the default `logged_at = now()`. Every line in a batch shares one timestamp, and `internal.log_lines` has no other ordering column. Lines within a batch have no defined order, and a page with a size limit can't resume from the middle of a batch.

The writer will give each line an increasing timestamp: the later of the current time and the previous line's timestamp plus 1µs. The data-plane controller's log writer does this within each batch ([`data-plane-controller/src/shared/logs.rs` L103-105](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/data-plane-controller/src/shared/logs.rs#L103-L105)). Carrying the timestamp across batches also keeps the order from one batch to the next. This needs no migration of the log table, which an ordering column would.

Consequences:

- Timestamps increase within one agent's log sink. Retries may use another agent; sinks do not coordinate their timestamps or commits.
- `logged_at` comes from the agent's clock instead of Postgres. Only the two-day expiry in `internal.delete_old_log_lines` compares it with the database clock, and clock skew doesn't matter at that scale.
- Lines written before the change keep shared timestamps until they expire, and a page boundary inside such a group can skip lines. This affects only discovers that ran in the two days before the change.

### Reading

`Discover.logs` provides best-effort pagination in `loggedAt` order. The cursor is the last line's timestamp, using the existing `TimestampCursor`. A delayed write from an earlier retry attempt can have a timestamp at or before a cursor already returned to the client, and subsequent pages will miss it. The page size has a default and no ceiling, like the other connections. A read covers one discover's lines, which the existing index on `token` already selects.

Lines reach the table asynchronously, so some can arrive after the status leaves `queued`, and there is no signal that a discover's logs are complete. `hasNextPage: false` means there are no more lines yet. A client can keep reading for a short time after the status becomes terminal to pick up late lines, but an empty page doesn't prove that none remain. A completion guarantee would need the log writer to acknowledge committed lines to the executor before it writes the status, and there is no such path today.

The reader takes a log token, and `LogLine` isn't specific to discovers, so a later `Publication.logs` reuses both. Nesting logs under the discover keeps the token out of the API. Log lines expire after two days.

## Authorization

The mutation checks `SpecEdit` with the caller's token. Copying a live capture also requires `CatalogRead` with that token. An unreadable live capture is treated as absent, so the fallback does not reveal its definition. The executor retains its own authorization checks with the user's full grants.

Consequences:

- Once capability-scoped tokens land, a token's restrictions apply when the discover is submitted. The executor doesn't see the token. It merges existing collections into the draft using the user's full `CatalogRead`, so a scoped token can then read collection specs in the draft that it couldn't read directly. The draft API has the same property, since ownership alone governs a draft's contents. Narrowing this belongs to the scoped-token work, for drafts, discovers, and publications together.
- The data-plane check uses legacy `read`, which requires every Viewer bit. The Editor bundle lacks `ViewDataPlanePrivateNetworking`, so a user whose grants reach the plane only through Editor fails the check, and a token scoped to Editor alone can't discover. The API matches the executor, because it must not accept a discover the executor will reject. Publications make the same check ([`publications/specs.rs` L960](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/control-plane-api/src/publications/specs.rs#L960)), so narrowing it is one decision for both.
- Reading a discover, its errors, and its logs requires owning its draft.

## Orphaned discover tasks

Deleting a draft deletes its `discovers` rows ([migration](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/supabase/migrations/01_compacted.sql#L7932)), but a queued row's task remains. The executor then fails to load the row ([`discovers/db.rs` L30-59](https://github.com/estuary/flow/blob/8898a170a1cc979711b2efc0ed35c25b81ade1ce/crates/control-plane-api/src/discovers/db.rs#L30-L59)) and logs a warning. The task is retried after each heartbeat timeout, without limit. The draft API's `deleteDraft` makes this easy to reach.

The executor will finish the task when its row no longer exists. The executor already decides when its task is done, so this is the smallest change.

A draft can also be deleted while its discover runs. Applying the outcome then fails, because the results can't be written into a deleted draft, and the task is retried. The retry finds the row gone and finishes. This costs one heartbeat timeout, so the fix doesn't add a second check when the outcome is applied.

A discover doesn't need to outlive its draft. Its results, the merged specs and the errors, live in the draft, so a retained row would hold only a status. The `discovers` table gets no user column.

## Delivery

The first PR has three commits:

1. `discover(id)` with `errors`, and the move of `JobStatus` into `models::discovers`.
2. `createDiscover`, including the transactional live-capture copy and discovery-specific plane selection.
3. Log ordering in the writer, and `Discover.logs`.

The second PR is the orphaned-task fix. Its tests delete the draft before the task runs, and while it runs, and check that the task finishes in both cases.

Tests use `sqlx::test` fixtures and snapshot whole GraphQL responses. Submission tests also check the draft, discover row, and queued task:

- Unauthenticated requests.
- A foreign draft and a missing draft, which give the same result.
- A staged capture takes precedence over a different live definition. Submission preserves the staged model, its metadata, and unrelated draft entries.
- Live fallback succeeds in both an empty draft and a draft containing unrelated specifications. The copy is visible before the executor runs and carries the live definition's `last_pub_id` as `expect_pub_id`. The test checks that the draft timestamp changes and unrelated entries remain unchanged.
- A capture missing from both the draft and readable live specs is rejected without creating a starter definition or a job.
- A staged deletion, `delete: true`, invalid capture model, another catalog type, or a non-image endpoint is rejected. A live capture of the same name must not bypass an invalid staged entry.
- A caller without `SpecEdit`, and a caller with `SpecEdit` but no `CatalogRead` for live fallback. An unreadable live capture and a missing live capture give the same error. The fallback's read requirement does not impose a new live-read requirement on a valid staged definition.
- Data-plane choice: omission and null use the live plane for an existing capture or the mapping's primary plane for a new capture. An explicit permitted plane overrides that selection for either case. The test checks the returned plane and the stored `data_plane_name`.
- An explicit plane outside an `acmeCo/` storage mapping is rejected, and the existing `ops/` exception remains supported. A missing plane and an unauthorized one give the same error. A new capture without a mapped primary plane fails when `dataPlane` is omitted.
- An unknown image, a tag that isn't a capture, and a tag whose spec has not succeeded.
- Policy defaults for missing, null, empty, and explicitly configured `autoDiscover`. Submission preserves the model's settings and writes the expected `update_only` value.
- JSON member order survives both copying a live definition and writing `endpoint_config` from either definition source.
- Submission failures leave the draft, its errors, its timestamp, and the job queue unchanged. Successful submissions retain existing draft errors until the executor applies its outcome.
- `discover(id)` for a foreign row and a missing one, historical status values, and `errors` after a failed run.
- Log pages whose boundaries fall inside one batch and between batches, in order.

Agent integration coverage runs GraphQL-created discovers from both a staged definition and a live fallback. It checks the merged capture and collections, including disabled new bindings and collection resets under the selected policy. A connector failure after successful live fallback leaves the copied capture in the draft and records the failure. A publication using the copied `expect_pub_id` rejects an intervening live change.

Testing also includes a manual run on a local stack, `.sqlx` regenerated from a clean database, and the regenerated schema.

[#3514](https://github.com/estuary/flow/pull/3514) overlaps this plan in four places:

- It edits `agent/src/discovers.rs`, which the `JobStatus` move also edits.
- It adds a `can_sign()` filter to the executor's data-plane check, which `createDiscover` mirrors.
- It rewrites validation's placement helpers and publications' data-plane resolution. Reuse the updated helpers while preserving discovery's explicit-plane override for existing captures.
- It replaces the executor's connector interface and the test harness's mock connectors, which the integration test uses.

`createDiscover`'s checks and the integration test follow whatever is on master when this work lands. Neither PR needs to wait for the other.

## Deferred work

The behavior decisions for this API are settled. These separate changes do not block implementation:

- Rename or alias `autoDiscover` to `discovery`, preserving the existing distinction between an absent stanza and an empty object.
- Revisit the data-plane check for the Editor bundle in a separate issue. This API keeps the executor's current authorization rule.
- Address shared-draft concurrency and scoped-token access to specifications that executors add to drafts. This plan documents the existing limitations without changing those contracts.
