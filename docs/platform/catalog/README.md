# Catalog

## Glossary

**Catalog**:
The complete set of captures, collections, materializations, and tests an
organization manages. A collection may additionally be a _derivation_.

**Model spec**:
A catalog entity in its declarative, user-authored form — the JSON/YAML a user
writes and edits.
_Avoid_: config, definition

**Built spec**:
The compiled form of an entity, carrying the extra specifics the runtime needs —
resolved connector configuration and the shard and journal templates — that the
control plane derives during a build.

**Live spec**:
The currently-committed form of an entity: its model and built specs in effect
right now, together with the identity of the publication that last changed them.
_Avoid_: current spec, deployed spec

**Draft**:
A proposed set of changes to model specs, edited before it is built and
committed.
_Avoid_: branch, changeset

**Build**:
The operation that verifies a draft's proposed changes against the live specs
they touch and compiles them into built specs — an outcome that is _not_ yet
committed.
_Avoid_: compile

**Commit**:
Atomically promoting a build's built specs to become the new live specs, subject
to optimistic concurrency.
_Avoid_: merge, deploy

**Publication**:
A build followed by a commit — the operation that takes a draft live. A _dry-run_
publication builds but does not commit.
_Avoid_: deploy, release

## What a catalog is

The catalog is the declarative model an organization manages. Everything a user
configures — the data they pull in, transform, and push out, and the tests that
guard it — is a catalog entity, named by a `/`-delimited path whose first
segment is its [tenant](../namespace/). There are four authored kinds:

- **Captures** and **materializations** — [tasks](../tasks/) that move data in
  from, and out to, external systems through [connectors](../connectors/).
- **Collections** — the data itself, schema-enforced (see
  [collections/](../collections/)). A collection may carry a _derivation_,
  making it a [derived collection](../tasks/derivations/) built by transforming
  other collections.
- **Tests** — fixtures asserting expected [derivation behavior](../tasks/tests/).

A derivation is a mode of a collection, not a separate entity: the authored
kinds are capture, collection, materialization, and test.

## The three forms of a spec

An entity exists in three forms as it moves through its lifecycle:

- A **model spec** is what the user authors — declarative, and free of runtime
  detail.
- A **built spec** is what a build compiles the model into — fully resolved,
  carrying the connector configuration and the Gazette shard and journal
  templates the [data plane](../data-plane/) needs to run it.
- A **live spec** is what is committed and in effect — the model and its built
  spec as of the last publication to change them.

A build turns model specs into built specs; a commit turns built specs into live
specs.

## The lifecycle

A change flows from a draft, through a build, to a commit.

A **draft** collects proposed changes to one or more model specs. Drafts are
assembled from files and their `import`s, or edited directly through the control
plane.

A **build** takes a draft and the live specs it touches and verifies it: schemas
and references resolve, keys and projections are sound, and each affected
capture, derivation, and materialization is confirmed with its connector.
Verification reaches beyond the drafted specs to the _connected_ live specs that
read from or write to them, so a change is caught when it would break a
downstream spec the draft never mentions. Any tests in scope are run. The build
compiles the result into built specs and reports an outcome — but changes
nothing: its built specs are not yet live.

A **commit** promotes a build's built specs to become the new live specs. It is
guarded by optimistic concurrency, so a commit fails rather than clobber a live
spec that another publication changed since the build began.

A **publication** is a build followed by a commit — the operation a user invokes
to take a draft live. A **dry-run** publication stops after the build, exposing
the built specs and any errors without changing anything live.

The mechanics — how drafts, builds, publications, and their queued and terminal
states are executed and recorded — belong to the
[control plane](../control-plane/#publications).

## Handoff to the data plane

Committing updates live specs; it does not touch a data plane. **Activation** —
installing a built spec's task or collection into its data plane so the runtime
executes it — happens asynchronously afterward, driven per live spec by
control-plane [controllers](../control-plane/#controllers). A publication reports
success once its specs are committed live, before they are activated. The
activation concept lives with [tasks](../tasks/).

## Where this lives

- `crates/models` — the model spec types (`CaptureDef`, `CollectionDef`, … and
  the `CatalogType` kinds)
- `crates/sources` — loading and resolving specs and their `import`s into a draft
- `crates/build`, `crates/validation` — the build: compiling and verifying a
  draft against live specs into built specs
- `crates/tables` — the draft, live, and built row model shared across the above
- `crates/activate` — the data-plane handoff for a committed build
- `go/protocols/flow/flow.proto` — the built-spec protobufs (`CaptureSpec`,
  `CollectionSpec`, `MaterializationSpec`, `TestSpec`)
