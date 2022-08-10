
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

The agent is a non-user-facing component which lives under [crates/agent/](crates/agent/) of this repo.  Its role is to find and execute all operations which are queued in various tables of our API.

Today this includes:

* Fetching connector details, such as open-graph metadata and endpoint / resource JSON-schemas.
* Running connector discovery operations to produce proposed catalog specifications.
* Publishing catalog drafts by testing and then activating them into the data-plane.

The agent is not very opinionated about where it runs and is architected for multiple instances running in parallel. Async operations in the database can be thought of as a task queue. Agents use ["select ... for update skip locked"](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) locking clauses in their queries to dequeue operations to execute within scoped transactions. This allows parallel agent instances to coordinate the assignment of operations between themselves, while also allowing for retries if an agent crashes (Postgres automatically releases any locks held by its transaction on session termination).

#### Flow Binaries

Many of the agents functions involve building, testing, activating, and deleting specifications into ephemeral data-planes or the production data-plane. The agent must also run connectors as part of verifications. It therefore expects an installation of Flow to be available and will shell out to its various binaries as needed.

Also required: [gsutil](https://cloud.google.com/storage/docs/gsutil), [sops](https://github.com/mozilla/sops), and [jq](https://stedolan.github.io/jq/).

### Data Plane Gateway

The [Data Plane Gateway](https://github.com/estuary/data-plane-gateway) serves a few endpoints which give access to Gazette RPCs. Notably, this allows querying for Shard status and directly reading Journals. This is used by the UI to check the status of a Shard.

The Control Plane issues access tokens via the `gateway_auth_token` function which grants users access to selected catalog prefixes.


## Local Development Guide

### Dependencies

You'll need:

* An installation of the [Supabase CLI](https://github.com/supabase/cli). Follow the installation instructions to install the latest version.
* A local checkout of [github.com/estuary/flow](https://github.com/estuary/flow) upon which you've run `make package`. This creates a directory of binaries `${your_checkout}/.build/package/bin/` which the control-plane agent refers to as `--bin-dir` or `$BIN_DIR`.
* A local checkout of [github.com/estuary/data-plane-gateway](https://github.com/estuary/data-plane-gateway).
* A local checkout of [github.com/estuary/ui](https://github.com/estuary/ui).
* A local checkout of [github.com/estuary/config-encryption](https://github.com/estuary/config-encryption).
* A local checkout of this repository.

### Start Supabase:

Run within your checkout of this repository (for example, ~/estuary/animated-carnival):
```console
supabase start
```

`supabase` configures itself from the [supabase/](supabase/) repo directory and loads schema migrations, extensions, and seed data sets. When `supabase start` finishes you have a full-fledged control plane API.

You can reset your DB into a pristine state:

```console
supabase db reset
```
Or, nuke it from orbit:
```console
supabase stop && supabase start
```

(Optional) run SQL tests if you're making schema changes.
```console
./supabase/run_sql_tests.sh
```

Supabase opens the following ports:

* 5431 is the PostgREST API.
* 5432 is the Postgres database.
* 5433 is the [Supabase UI](http://localhost:5433).
* 5434 is the email testing server (not used right now).

Directly access your postgres database:

```console
psql postgres://postgres:postgres@localhost:5432/postgres
```

### Start `temp-data-plane`:

Suppose that `${BIN_DIR}` is the `make package` binaries under `.build/package/bin` of your Flow checkout.
You start a `temp-data-plane` which runs a local instance of `etcd`, a `gazette` broker, and the Flow Gazette consumer:

```console
~/estuary/flow/.build/package/bin/flowctl-go temp-data-plane --log.level warn
```

A `temp-data-plane` runs the same components and offers identical APIs to a production data plane with one key difference: unlike a production data-plane, `temp-data-plane` is ephemeral and will not persist fragment data to cloud storage regardless of [JournalSpec](https://gazette.readthedocs.io/en/latest/brokers-journalspecs.html) configuration. When you stop `temp-data-plane` it discards all journal and shard specifications and fragment data. Starting a new `temp-data-plane` is then akin to bringing up a brand new, empty cluster.

### Start the `data-plane-gateway`:

Build the `data-plane-gateway` binary:

```console
cd data-plane-gateway/
go install .
go build .
```

_Note: It is not necessary to install all the protoc tooling or run `make`. Those are only necessary for modifying the generated code within the gateway._

Start the gateway:

```console
data-plane-gateway
```

_Note: The gateway allows for configuring the port, the Flow service ports, the signing secret, and the CORS settings. The defaults should work out of the box._

### Build `fetch-open-graph`:

Build the fetch-open-graph helper to the same location where the flow binaries live. This is the same path that will be provided to the agent using `--bin--dir` argument:

```console
cd fetch-open-graph/
go build -o ~/estuary/flow/.build/package/bin/
```

### Start the `agent`:

Again from within your checkout of this repo:

```console
RUST_LOG=info cargo run -p agent -- --bin-dir ~/estuary/flow/.build/package/bin/
```

`agent` requires several arguments. Consult `--help`:
```console
cargo run -p agent -- --help
```
Typically the defaults are directly useable for local development.

### Connectors

On startup, you'll see the agent start fetching images for a handful of
connector tags that are in the DB's seed schema.

Add all production connectors via:

```console
psql postgres://postgres:postgres@localhost:5432/postgres -f ./scripts/seed_connectors.sql
```

We're attempting to keep this file up-to-date with the production DB,
so if you spot drift please update it. Be aware this can pull down a lot of
docker images as the agent works through the connector backlog. You may want
to manually add only the connectors you're actively working with.


### Running the UI:

In your UI repo checkout you'll currently need to tweak `~/estuary/ui/.env`.
Look for sections that say "Uncomment me for local development" and "Comment me", and follow the directions.

`npm install` is required on first run or after a git pull:
```console
npm install
```

Then you can start a local instance of the UI as:
```console
npm start
```

The UI will open a browser and navigate to your dashboard at [http://localhost:3000](http://localhost:3000).
Your installation is seeded with three existing users:

* alice@example.com
* bob@example.com
* carol@example.com

To login with a Magic Link you need to enter an email and then your local will "send" an email. To check this email you need to use Inbucket [http://localhost:5434/](http://localhost:5434/) and click on the link provided in the email.

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

We use schemas `public` and `internal`, so both should be compared. **Do not** run this script directly. Read it, understand it, make sure it's sensible, and then run it. The production secret can be found in the sops-encrypted file `supabase/secret.yaml`.

Migrations should be applied via:
```console
psql postgresql://postgres:${DB_SECRET}@db.eyrcnmuzzyriypdajwdk.supabase.co:5432/postgres
```

**Do not** use the Supabase UI for applying migrations as they run as a different user from `postgres`, which changes the owner and confuses the heck out of pgdiff.
