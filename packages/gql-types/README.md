# @estuarydev/gql-types

TypeScript types and GraphQL SDL for the Estuary control plane API.

The schema is defined in Rust via `async-graphql` in `crates/control-plane-api/`
and extracted to `crates/flow-client/control-plane-api.graphql` at build time.
This package makes that schema and generated base TypeScript types available to frontend.

## Publishing

CI publishes to npm automatically when the version in `package.json` is bumped and merged to `master`.

To release a new version:

1. Update the schema (modify the Rust GraphQL types, then run
   `cargo build -p flow-client --features generate` to regenerate the SDL)
2. Bump the version in this `package.json` following semver:
   - Patch (`0.1.1`): bug fixes, doc-only changes
   - Minor (`0.2.0`): new types, fields, queries, or mutations (additive)
   - Major (`1.0.0`, `2.0.0`): removed or renamed types/fields (breaking)
3. Commit both the updated SDL and the version bump
4. Merge to `master` — CI publishes automatically

### Pre-releases

For parallel frontend/backend development on a feature branch:

1. Set the version to e.g. `0.2.0-invite-links.0` in `package.json`
2. Trigger the workflow manually via `workflow_dispatch` on your branch
3. The frontend can install `@estuarydev/gql-types@0.2.0-invite-links.0`

## Local development

To use unpublished schema changes locally:

```bash
# In the flow repo — regenerate SDL from Rust (if schema changed)
cargo build -p flow-client --features generate

# Build the package and create a global npm link
./scripts/link-gql-types.sh

# In the frontend repo — use the local version
npm link @estuarydev/gql-types
```

Re-run `./scripts/link-gql-types.sh` after each schema change.
Run `npm unlink @estuarydev/gql-types` in the frontend to revert to the published version.

## Frontend usage

Install the package:

```bash
npm install @estuarydev/gql-types
```

Import types directly:

```typescript
import type { LiveSpec, Alert, Capability } from "@estuarydev/gql-types";
```

The raw SDL is also available for local codegen (e.g. generating typed urql hooks):

```
node_modules/@estuarydev/gql-types/schema/control-plane-api.graphql
```
