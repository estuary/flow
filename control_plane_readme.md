# Flow Control-Plane

The Flow control-plane orchestrates the Flow data-plane, controlling the specifications which are running in the data-plane, their activations, deletions, and so on. It provides APIs through which users can draft changes to specifications, holistically test their drafts, publish them as live specifications into the data-plane, monitor their execution, and understand the history of specification changes over time.

## Architecture

The control-plane consists of the following components:

### Supabase

Supabase is itself an opinionated bundling of Postgres, [PostgREST](https://postgrest.org/en/stable/) for REST APIs, the GoTrue authentication service, and of other useful open-source components. [Consult the Supabase architecture](https://supabase.com/docs/architecture).

Supabase powers all elements of our public-facing API and powers authentication (AuthN), authorization (AuthZ), and user-driven manipulation of the control-plane database.

Much of the control-plane business logic lives in SQL schemas under [supabase/migrations/](supabase/migrations/) of this repo, and wherever possible the various constraints and checks of the platform are encoded into and enforced by these SQL schemas.

Not everything can be done in SQL. More complex interactions, validations, and requests for privileged actions are represented as asynchronous operations within our schema. The user initiates an operation through an API request which records the desired operation in the DB. A control-plane "agent" then executes the operation on the user's behalf, and communicates the operation status and results through the database.

### Flow UI & CLI

Flow's user-interface is a single-page React application hosted at [dashboard.estuary.dev](https://dashboard.estuary.dev). It's repository is [github.com/estuary/ui](https://github.com/estuary/ui). The UI uses the Supabase APIs.

We also develop a full featured command-line interface client `flowctl`, which lives at [estuary/flowctl](https://github.com/estuary/flowctl).

### Control-plane Agent

The agent is a non-user-facing component which lives under [crates/agent/](crates/agent/) of this repo.  Its role is to find and execute all operations which are queued for controllers, or in various tables of our legacy API.

Today this includes:

* `internal.tasks`: Specifically tasks of type `2` are for live specs controllers, which update the `controller_jobs` table. Unlike the others, this is not a user-facing API and is _not_ considered legacy.
* `connector_tags`: Fetching connector details, such as endpoint / resource JSON-schemas.
* `discovers`: Running connector discovery operations to produce proposed catalog specifications.
* `publications`: Publishing catalog drafts by testing and then activating them into the data-plane.
* `applied_directives`: User and tenant management actions.

Note that those table-based APIs are legacy, except for `controller_jobs`, and we intend to gradually phase them out as we introduce equivalend GraphQL APIs.

### control-plane-api

This crate lives under `crates/control-plane-api/`, and includes REST and GraphQL endpoints, and also exports rust functions for use by control plane agent. We intent to add most new control plane functionality as GraphQL queries and mutations in this crate.

Notably, this includes the various authorization endpoints, which are used by data planes.

### Config encryption and OAuth APIs

- [github.com/estuary/config-encryption](https://github.com/estuary/config-encryption): Used by flowctl to encrypt endpoint configs, also used by the OAuth function for the same
- OAuth function (`supabase/functions/oauth/`): Used by the UI to encrypt endpoint configs and handle OAuth authentication. This calls the config-encryption service

#### Flow Binaries

Many of the agents functions involve building, testing, activating, and deleting specifications into ephemeral data-planes or the production data-plane. The agent must also run connectors as part of verifications. It therefore expects an installation of Flow to be available and will shell out to its various binaries as needed.

Also required: [gsutil](https://cloud.google.com/storage/docs/gsutil), [sops](https://github.com/mozilla/sops), and [jq](https://stedolan.github.io/jq/).

## Development

We use the `sqlx` crate for interacting with postgres, which parses the queries provides some type safety. This requires the database to be available at compile time, unless the `SQLX_OFFLINE=1` env variable is set. In offline mode, which is used in CI, it will use cached data from `sqlx-data.json`. This means that `sqlx-data.json` must be updated whenever any queries are added or modified. To do that, run `cargo sqlx prepare --merged` from the workspace root.

Note: This currently requires `sqlx-cli` version `0.6.3`. We intend to update the sqlx dependency, and then the above command will change to `cargo sqlx prepare --workspace`.

### Building on M1

* To cross-compile `musl` binaries from a darwin arm64 (M1) machine, you need to install `musl-cross` and link it:
  ```
  brew install filosottile/musl-cross/musl-cross
  sudo ln -s /opt/homebrew/opt/musl-cross/bin/x86_64-linux-musl-gcc /usr/local/bin/musl-gcc
  ```

* Install GNU `coreutils` which are used in the build process using:

  ```
  brew install coreutils
  ```

* If you encounter build errors complaining about missing symbols for x86_64 architecture, try setting the following environment variables:
  ```
  export GOARCH=arm64
  export CGO_ENABLED=1
  ```

* If you encounter build errors related to openssl, you probably have openssl 3 installed, rather than openssl 1.1:
  ```
  $ brew uninstall openssl@3
  $ brew install openssl@1.1
  ```
  Also make sure to follow homebrew's prompt about setting `LDFLAGS` and `CPPFLAGS`

* If you encounter build errors complaining about `invalid linker name in argument '-fuse-ld=lld'`, you probably need to install llvm:
  ```
  $ brew install llvm
  ```
  Also make sure to follow homebrew's prompt about adding llvm to your PATH

## Production Migrations

This area is a work-in-progress -- it's Johnny's evolving opinion which we may disregard or change:

The desired practice is that we maintain the "ideal" schema in [supabase/migrations/](supabase/migrations/). We keep a single representation of tables and views as we _wish_ them to be, even if that's not as they are.

Then we _converge_ the production database towards this desired state by diffing it and identifying migrations to run. Tooling can help us identify incremental changes that must be made to the production database.

This practice stands in contrast with the practice of keeping additive-only migrations with `ALTER TABLE` statements. We may do this as a short-term measure while developing a migration strategy, but it's an ephemeral migration script which is removed once applied.

The rationale is that migrations are a point-in-time action that, once taken, doesn't need to be revisited. However **every** developer is regularly consulting SQL schema, so it's important to optimize for human readers rather than the particular database order that things happened to be historically applied in.

Example of using pgadmin to obtain a schema diff:
```console
docker run \
  --network supabase_network_animated-carnival \
  --rm -it supabase/pgadmin-schema-diff:cli-0.0.4 \
  --schema public \
  postgresql://postgres:postgres@supabase_db_animated-carnival:5432/postgres \
  postgresql://postgres:${DB_SECRET}@db.eyrcnmuzzyriypdajwdk.supabase.co:5432/postgres
```

Note that pgAdmin 4's schema-diff currently produces extra REVOKE/GRANT migrations for tables that appear only due to different orderings of access privileges within the postgres catalog. See issue https://redmine.postgresql.org/issues/6737. These are annoying but can be ignored: even if you apply them they come back due to a presumed Supabase maintenance action. Example:

```
REVOKE ALL ON TABLE public.draft_errors FROM authenticated;
REVOKE ALL ON TABLE public.draft_errors FROM service_role;
GRANT SELECT, DELETE ON TABLE public.draft_errors TO authenticated;

GRANT ALL ON TABLE public.draft_errors TO service_role;
```

We use schemas `public` and `internal`, so both should be compared. **Do not** run this script directly. Read it, understand it, make sure it's sensible, and check it in under `supabase/pending/` in the same PR that that updates the migrations. The `pending/` migrations need to be run manually. Do not forget to do this! The production secret can be found in the sops-encrypted file `supabase/secret.yaml`.

Migrations should be applied via:
```console
psql postgresql://postgres:${DB_SECRET}@db.eyrcnmuzzyriypdajwdk.supabase.co:5432/postgres
```

**Do not** use the Supabase UI for applying migrations as they run as a different user from `postgres`, which changes the owner and confuses the heck out of pgdiff.
