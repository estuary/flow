# Snowflake CDC Connector

This connector captures change events from source tables in a Snowflake database.

## Prerequisites

To use this connector, you'll need:

* A Snowflake account that includes:
    * A target database containing the tables you want to capture from.
    * A [virtual warehouse](https://docs.snowflake.com/en/user-guide/warehouses) which the connector can use to execute queries.
    * A [schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html) which will hold [streams](https://docs.snowflake.com/en/user-guide/streams-intro) and staging tables managed by the connector. The default name for this schema is `ESTUARY_STAGING` unless overridden in the capture's advanced configuration.
    * A user with access grants for these resources, as well as authorization to read from the desired source tables, and to create streams and transient tables in the staging schema based on the source tables.
* The host URL for your Snowflake account. This is formatted using your [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used), and might look something like `sg31386.snowflakecomputing.com` or `df98701.us-central1.gcp.snowflakecomputing.com`.

See the [script below](#setup) for details.

## Setup

To set up a user account and warehouse for use with the Snowflake CDC connector,
copy and paste the following script into the Snowflake SQL editor. Modify the
variable declarations in the first few lines to set the password and optionally
customize the names involved.

```sql
set database_name = 'SOURCE_DB';         -- The database to capture from
set warehouse_name = 'ESTUARY_WH';       -- The warehouse to execute queries in
set estuary_user = 'ESTUARY_USER';       -- The name of the capture user
set estuary_password = 'secret';         -- The password of the capture user
set estuary_role = 'ESTUARY_ROLE';       -- A role for the capture user's permissions

-- Create a role and user for Estuary
create role if not exists identifier($estuary_role);
grant role identifier($estuary_role) to role SYSADMIN;
create user if not exists identifier($estuary_user)
  password = $estuary_password
  default_role = $estuary_role
  default_warehouse = $warehouse_name;
grant role identifier($estuary_role) to user identifier($estuary_user);

-- Create a warehouse for Estuary and grant access to it
create warehouse if not exists identifier($warehouse_name)
  warehouse_size = xsmall
  warehouse_type = standard
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;
grant USAGE
  on warehouse identifier($warehouse_name)
  to role identifier($estuary_role);

-- Grant Estuary access to read from all tables in the database and to create a staging schema
grant CREATE SCHEMA, MONITOR, USAGE
  on database identifier($database_name)
  to role identifier($estuary_role);
grant USAGE
  on future schemas in database identifier($database_name)
  to role identifier($estuary_role);
grant USAGE
  on all schemas in database identifier($database_name)
  to role identifier($estuary_role);
grant SELECT
  on future tables in database identifier($database_name)
  to role identifier($estuary_role);
grant SELECT
  on all tables in database identifier($database_name)
  to role identifier($estuary_role);

commit;
```

Be sure to run the entire script with the "Run All" option.

## Configuration

You can configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Snowflake CDC source connector.

### Endpoint Properties

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/host`** | Host URL | The Snowflake Host used for the connection. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol). | string | Required |
| **`/account`** | Account | The Snowflake account identifier | string | Required |
| **`/database`** | Database | The name of the Snowflake database to capture from | string | Required |
| **`/user`** | User | The Snowflake user login name | string | Required |
| **`/password`** | Password | The password for the specified login user | string | Required |
| `/warehouse` | Warehouse | The Snowflake virtual warehouse used to execute queries. The default warehouse for the user will be used if this is blank. | string |  |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/flowSchema` | Flow Schema | The schema in which Flow will create and manage its streams and staging tables. | string  | ESTUARY_STAGING |

### Binding Properties

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | The name of the table to capture | string | Required |
| **`/schema`** | Schema | The schema in which the table resides | string | Required |

### Polling Interval

Keeping a Snowflake compute warehouse active 24/7 can be prohibitively expensive for many users,
so the Snowflake CDC connector is designed to poll for changes at a configurable interval, at
which time it will capture into Flow all new changes since the previous execution. This polling
interval is set to 5 minutes by default, in an attempt to strike a balance between cost savings
while still providing "good enough" capture latency for most streaming uses. The interval may
be configured by editing the task spec `interval` property [as described here](https://docs.estuary.dev/concepts/captures/#specification).

Specifying a smaller interval can provide even lower capture latencies but is likely to incur
higher costs for Snowflake warehouse usage. A higher interval will reduce Snowflake costs by
allowing the warehouse to be idle for longer, in cases where it's okay for the captured data
to lag the source dataset by a few hours. Note that regardless of the polling interval the
output collections will contain an accurate representation of the source tables up to some
moment in time, the interval merely controls how frequent and fine-grained the updates are.

### Sample Configuration

```yaml
captures:
  ${prefix}/source-snowflake:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-snowflake:v1
        config:
          host: cf22902.us-central1.gcp.snowflakecomputing.com
          account: cf22902
          database: SOURCE_DB
          user: ESTUARY_USER
          password: secret
    bindings:
      - resource:
          schema: ${schema_name}
          table: ${table_name}
        target: ${prefix}/collection_name
    interval: 30m
```
