# Control Plane

The Control Plane orchestrates actions taken by API users against the Data Plane.

## Getting Started

Users can manage the Control Plane using `flowctl`:

```bash
# Setup the database
$ flowctl control-plane setup --config path/to/config.toml
# Seed the database
$ flowctl control-plane seed --config path/to/config.toml
# Run the API Server
$ flowctl control-plane serve --config path/to/config.toml
```
### Application Configuration

All of these commands require a configuration file. In the future, we'll have a
command to generate one, but for now, examples of the application configuration
can be found in `crates/control/config/`. One of the critical components is the
database connection information. You can include this information in the
configuration file, or use a `DATABASE_URL` environment variable.

* All configuration files can be overridden with environment variables. They all
follow the same scheme, beginning with `CONTROL_` and using SCREAMING_SNAKE_CASE for the key name: `CONTROL_{KEY}`.
    * eg. `CONTROL_APP_ENV=test`.
* If you are trying to override a nested property of the configuration, you can
use `__` to delimit the levels, `APP_LEVEL_ONE__LEVEL_TWO`.
    * eg.
`CONTROL_APPLICATION__HOST=example.com`.


## Local Development

Need to work on the Control Plane itself? Start here.

### Concerning Databases

They are very concerning. :grimacing:


#### Which Database?

The Control Plane is going to need a running Postgres database for it to
meaningfully do any work. You can use docker-compose to launch one that's going
to work right out of the box, or you can bring your own.

```bash
$ docker-compose up --detach postgres
```

If you prefer to manage your own database process, you can simply set a
`DATABASE_URL` environment variable and subsequent commands should respect that.


#### Setup

We use [SQLx](https://github.com/launchbadge/sqlx) to query and manage our
Postgres database.

For development, you can create and migrate the database with:

```bash
$ sqlx database setup
```

If you have an outdated version of the database schema in your database, you can
run pending migrations with:

```bash
$ sqlx migrate run
```

Or give up subtly and blow away the database entirely:

```bash
$ sqlx database reset
```

The `sqlx` cli tool has quite a few useful additonal commands you may want to
familiarize yourself with.


#### SQLX_OFFLINE mode

One of SQLx's nifty features is that it can verify your SQL queries at compile
time. This is great for helping with correctness, but is a potential
bootstrapping problem. To mitigate this, by default, it will use "offline mode"
and read cached query metadata from `sqlx-data.json`.

To switch to "online" query-verification mode (you're actively developing,
making changes to the schema or queries):

```bash
$ export SQL_OFFLINE=false
```

At the top of the flow project, we set `SQLX_OFFLINE=true` so that flow builds
do not necessarily try to connect to the database. This allows us to build
`flowctl` without a database connection. The second stage of CI will run the
integration tests.

If you change the database schema and/or the contents of any sqlx `query!`
macros, you will need to regenerate the `sqlx-data.json` file so that offline
users can still compile the project.

```bash
$ cargo sqlx prepare -- --lib
```

The second stage of CI will verify that this query cache file is up to date with
the latest schema and queries being used in the project.


#### Integration Tests

This will require an active database connection to successfully compile the
project. If you get strange errors eminating from `control::repo` modules, you
may want to double check that your database is available and migrated.

No amount of caching can help us run tests though. A Postgres instance is
required integration testing.

```bash
$ cargo test --all-features
# Also aliased as `cargo test-all`!
```

The `--all-features` is needed because the integration tests require certain
features to be present.  The features are required so that a `cargo test --all`
from the repo root will not try to run control plane integration tests.

Each integration test will create an isolated database within the postgres
instance named for the particular test. These are dropped and recreated at the
beginning of each test run. This allows you to connect to your Postgres and
inspect a specific tests's database state after a test failure.


##### Snapshot Testing

We use [insta](https://github.com/mitsuhiko/insta) for snapshot testing. This
allows us to easily verify the responses for various test scenarios without
tedious assertion checking. When the tests run, the output is compared against
the snapshots. Differences can be reviewed and approved/rejected from the CLI.

```bash
# Review changed changed snapshots
$ cargo insta review
# Run tests and immediately review any diffs
$ cargo insta test --review --all-features
# Also aliased as `cargo review-all`!
```

##### Test Databases Connections

**Note:** If you try to run the tests _without_ running `sqlx database setup` first,
you may get a postgres connection error when the tests attempt to create the
test database. In this case, you *must* set the `DATABASE_URL` to allow
accessing a real database within your Postgres instance.

```bash
$ DATABASE_URL=postgres://flow:flow@localhost:5432/postgres cargo test-all
```

#### Running the Server

You can launch a development server with cargo:

```bash
$ cargo run
```

Or for a fast dev feedback cycle:

```bash
$ cargo watch --why -x 'run -- --log.level=info,tower_http=debug,sqlx=warn'
```

This uses `crates/control/src/main.rs`, which is exclusively for local dev
environments. `main.rs` is _not_ used for production. The "official" way to run
control plane is to run `flowctl control-plane serve`.

#### UI Integration

The Control Plane can be used to provide data for the
[UI](https://github.com/estuary/ui/). Follow the setup instructions for the UI
and configure it to connect to the locally launched Control Plane API.

#### Manual Testing Recipes

To see the raw responses from your console, you can use `curl` or an equivalent
tool like [`xh`](https://github.com/ducaale/xh) and pair it with `jq` to explore
the API.

###### Fetch a Session Token:

```bash
$ curl -s -H "Content-Type: application/json" localhost:3000/sessions/local -d '{"auth_token": "batman"}' | jq '.'
```

###### One-stop shop for setting an AUTH variable after login:

```bash
$ export AUTH=$(curl -s -H "Content-Type: application/json" localhost:3000/sessions/local -d '{"auth_token": "batman"}' | jq '.data.attributes | "\(.account_id):\(.token)" | @base64' -r)
```

###### Send an authenticated request:

```bash
$ curl -v -H "Authorization: Basic $AUTH" http://127.0.0.1:3000/accounts | jq .
```

###### Use `xh` sessions:

`xh` may be slightly more concise than equivalent `curl` commands, but YMMV.

```bash
# Login
$ xh -j :3000/sessions/local auth_token=batman
# Send the basic auth once and save it to a session
$ xh -j --session batman :3000/accounts/JqxL8V6ABAE -a JqxL8V6ABAE:<TOKEN>
# Use the session in subsequent requests
$ xh -j --session batman :3000/accounts/JqxL8V6ABAE
```

###### Follow the trail of resource links:

```bash
# Grab the link to the Postgres connector images
$ xh -j --session batman :3000/connectors | jq '.data[] | select(.attributes.name | contains("Postgres") ) | .links.images'
"http://127.0.0.1:3000/connectors/JqyiG4kABAU/connector_images"

# Grab the link to the first image's spec
$ xh --session batman :3000/connectors/JqyiG4kABAU/connector_images | jq '.data[0].links.spec'
"http://127.0.0.1:3000/connector_images/JqyiG4mABAY/spec"

# Grab the link for generating a discovered catalog
$ xh --session batman :3000/connector_images/JqyiG4mABAY/spec | jq ".data.links.discovered_catalog"
"http://127.0.0.1:3000/connector_images/JqyiG4mABAY/discovered_catalog"

# Grab the collection names discovered in a Postgres database
$ xh -j --session batman \
  http://127.0.0.1:3000/connector_images/JqyiG4mABAY/discovered_catalog \
  name=batman/control_development \
  config:=@path/to/postgres-config.json \
  | jq '.data[] | select(.type == "discovered_catalog").attributes.data.collections | keys'
[
  "batman/_sqlx_migrations",
  "batman/accounts",
  "batman/connector_images",
  "batman/connectors",
  "batman/credentials"
]
```
