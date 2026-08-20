---
description: Use the Snowflake connector to materialize Estuary collections into Snowflake tables. Configure delta updates for Snowpipe Streaming, JWT auth, sync schedules, and timestamp types.
---

import ReactPlayer from "react-player";

# Snowflake

This connector materializes Estuary collections into tables in a Snowflake database.
It allows both standard and [delta updates](#delta-updates). [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview) is additionally available for delta update bindings.

The connector first uploads data changes to a [Snowflake table stage](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html#table-stages).
From there, it transactionally applies the changes to the Snowflake table.

<ReactPlayer controls url="https://www.youtube.com/watch?v=nC_zDUz4SQw" />

## Prerequisites

To use this connector, you'll need:

* A Snowflake account that includes:
    * A target database, to which you'll materialize data
    * A [schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html) — a logical grouping of database objects — within the target database
    * A virtual warehouse
    * A user with a role assigned that grants the appropriate access levels to these resources.
    * The correct timezone setting (see [Timestamp Data Type Mapping](#timestamp-data-type-mapping))
    * The `QUOTED_IDENTIFIERS_IGNORE_CASE` parameter must not be enabled for the Estuary user.

    See the [script below](#setup) for details.
* Know your Snowflake account's host URL. This is formatted using your [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used),
for example, `orgname-accountname.snowflakecomputing.com`.
* At least one Estuary collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

To meet the prerequisites, copy and paste the following script into the Snowflake SQL editor, replacing the variable names in the first five lines.

If you'd like to use an existing database, warehouse, and/or schema, be sure to set
`database_name`, `warehouse_name`, and `estuary_schema` accordingly. If you specify a new name, the script will create the item for you. You can set `estuary_role`
and `estuary_user` to whatever you'd like.

Check the **All Queries** check box, and click **Run**.

```sql
set database_name = 'ESTUARY_DB';
set warehouse_name = 'ESTUARY_WH';
set estuary_role = 'ESTUARY_ROLE';
set estuary_user = 'ESTUARY_USER';
set estuary_schema = 'ESTUARY_SCHEMA';
-- create role and schema for Estuary
create role if not exists identifier($estuary_role);
grant role identifier($estuary_role) to role SYSADMIN;
-- Create snowflake DB
create database if not exists identifier($database_name);
use database identifier($database_name);
create schema if not exists identifier($estuary_schema);
-- create a user for Estuary
create user if not exists identifier($estuary_user)
default_role = $estuary_role
default_warehouse = $warehouse_name;
grant role identifier($estuary_role) to user identifier($estuary_user);
-- Estuary requires case-sensitive quoted identifiers (e.g. "_meta/op").
alter user identifier($estuary_user) set QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
grant all on schema identifier($estuary_schema) to identifier($estuary_role);
-- create a warehouse for estuary
create warehouse if not exists identifier($warehouse_name)
warehouse_size = xsmall
warehouse_type = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true;
-- grant Estuary role access to warehouse
grant USAGE
on warehouse identifier($warehouse_name)
to role identifier($estuary_role);
-- grant Estuary access to database
grant CREATE SCHEMA, MONITOR, USAGE on database identifier($database_name) to role identifier($estuary_role);
-- change role to ACCOUNTADMIN for STORAGE INTEGRATION support to Estuary (only needed for Snowflake on GCP)
use role ACCOUNTADMIN;
grant CREATE INTEGRATION on account to role identifier($estuary_role);
use role sysadmin;
COMMIT;
```

### Key-pair Authentication

As username and password authentication was deprecated in April 2025, you need to authenticate
using [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth), also known as JWT authentication.

To set up your user for key-pair authentication, first generate a key-pair in your shell:
```bash
# generate a private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
# generate a public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
# read the public key and copy it to clipboard
cat rsa_key.pub

-----BEGIN PUBLIC KEY-----
MIIBIj...
-----END PUBLIC KEY-----
```

Then assign the public key with your Snowflake user using these SQL commands:
```sql
ALTER USER identifier($estuary_user) SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...'
```

Verify the public key fingerprint in Snowflake matches the one you have locally:
```sql
DESC USER identifier($estuary_user);
SELECT TRIM((SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "property" = 'RSA_PUBLIC_KEY_FP'), 'SHA256:');
```

Then compare with the local version:
```bash
openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```

Now you can use the generated _private key_ when configuring your Snowflake connector.

:::tip
Key-pair authentication is required for delta updates bindings to use Snowpipe Streaming.
:::

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the below properties to configure a Snowflake materialization, which will direct one or more of your Estuary collections to new Snowflake tables.

### Properties

#### Endpoint

| Property                     | Title               | Description                                                                                                                                                     | Type   | Required/Default |
|------------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/host`**                  | Host (Account URL)  | The Snowflake Host used for the connection. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol).                                  | string | Required         |
| **`/database`**              | Database            | Name of the Snowflake database to which to materialize                                                                                                          | string | Required         |
| **`/schema`**                | Schema              | Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables | string | Required         |
| `/warehouse`                 | Warehouse           | Name of the data warehouse that contains the database                                                                                                           | string |                  |
| `/role`                      | Role                | Role assigned to the user                                                                                                                                       | string |                  |
| **`/timestamp_type`** | Snowflake Timestamp Type | Controls how timestamp columns are stored in Snowflake. See [Timestamp Data Type Mapping](#timestamp-data-type-mapping) for usage. | string | Required |
| `/hardDelete` | Hard Delete | If this option is enabled, items deleted in the source will also be deleted from the destination. Otherwise, `_meta/op` in the destination will signify whether rows have been deleted (soft-delete). | boolean | `false` |
| **`/credentials`**           | Credentials         | Credentials for authentication                                                                                                                                  | object | Required         |
| **`/credentials/auth_type`** | Authentication type | `jwt` is the only supported authentication method currently                                                                                                     | string | Required         |
| **`/credentials/user`**      | User                | Snowflake username                                                                                                                                              | string | Required         |
| `/credentials/password`      | Password            | Deprecated                                                                                                                                                      | string | Deprecated       |
| `/credentials/private_key`    | Private Key         | Required if using jwt authentication                                                                                                                            | string | Required         |
| `/advanced/disableFieldTruncation` | Disable Field Truncation | Disables truncation of large materialized fields | boolean | |
| `/advanced/no_flow_document` | Exclude Flow Document | When enabled, the root document will not be required for standard updates. See [excluding flow_document with standard updates](/guides/customize-materialization-fields/#excluding-flow_document-with-standard-updates) for details. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table name | string | Required |
| `/schema` | Alternative Schema | Alternative schema for this table | string |  |
| `/delta_updates` | Delta updates | Whether to use standard or [delta updates](#delta-updates) | boolean |  |

### Sample

Key-pair authentication:

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
  	    connector:
    	    config:
              database: acmeCo_db
              host: orgname-accountname.snowflakecomputing.com
              schema: acmeCo_flow_schema
              warehouse: acmeCo_warehouse
              timestamp_type: TIMESTAMP_LTZ
              credentials:
                auth_type: jwt
                user: snowflake_user
                private_key: |
                  -----BEGIN PRIVATE KEY-----
                  MIIEv....
                  ...
                  ...
                  ...
                  ...
                  ...
                  -----END PRIVATE KEY-----
    	    image: ghcr.io/estuary/materialize-snowflake:v4
  # If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
    source: ${PREFIX}/${source_collection}
```

(DEPRECATED) User and password authentication:

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
  	    connector:
    	    config:
              database: acmeCo_db
              host: orgname-accountname.snowflakecomputing.com
              schema: acmeCo_flow_schema
              warehouse: acmeCo_warehouse
              timestamp_type: TIMESTAMP_LTZ
              credentials:
                auth_type: user_pasword
                user: snowflake_user
                password: secret
    	    image: ghcr.io/estuary/materialize-snowflake:v4
  # If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
    source: ${PREFIX}/${source_collection}
```

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](/reference/materialization-sync-schedule).

Snowflake compute is [priced](https://www.snowflake.com/pricing/) per second of
activity, with a minimum of 60 seconds. Inactive warehouses don't incur charges.
To keep costs down, you'll want to minimize your warehouse's active time.

To accomplish this, we recommend a two-pronged approach:

* [Configure your Snowflake warehouse to auto-suspend](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse.html#:~:text=Specifies%20the%20number%20of%20seconds%20of%20inactivity%20after%20which%20a%20warehouse%20is%20automatically%20suspended.) after 60 seconds.

   This ensures that after each transaction completes, you'll only be charged for one minute of compute, Snowflake's smallest granularity.

   Use a query like the one shown below, being sure to substitute your warehouse name:

   ```sql
   ALTER WAREHOUSE ESTUARY_WH SET auto_suspend = 60;
   ```

* Configure the materialization's **Sync Schedule** based on your requirements for data freshness.


## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.

Enabling delta updates will prevent Estuary from querying for documents in your Snowflake table, which can reduce latency and costs for large datasets.
If you're certain that all events will have unique keys, enabling delta updates is a simple way to improve
performance with no effect on the output.
However, enabling delta updates is not suitable for all workflows, as the resulting table in Snowflake won't be fully reduced.

You can enable delta updates on a per-binding basis:

```yaml
    bindings:
  	- resource:
      	table: ${table_name}
        delta_updates: true
    source: ${PREFIX}/${source_collection}
```
## Performance considerations

### Optimizing performance for standard updates

When using standard updates for a large dataset, the [collection key](../../../concepts/collections.md#keys) you choose can have a significant impact on materialization performance and efficiency.

Snowflake uses [micro partitions](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions.html) to physically arrange data within tables.
Each micro partition includes metadata, such as the minimum and maximum values for each column.
If you choose a collection key that takes advantage of this metadata to help Snowflake prune irrelevant micro partitions,
you'll see dramatically better performance.

For example, if you materialize a collection with a key of `/user_id`, it will tend to perform far worse than a materialization of `/date, /user_id`.
This is because most materializations tend to be roughly chronological over time, and that means that data is written to Snowflake in roughly `/date` order.

This means that updates of keys `/date, /user_id` will need to physically read far fewer rows as compared to a key like `/user_id`,
because those rows will tend to live in the same micro-partitions, and Snowflake is able to cheaply prune micro-partitions that aren't relevant to the transaction.

### Snowpipe Streaming

[Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview) is the lowest-latency method to load data into Snowflake.
Snowpipe Streaming is used by default for [delta updates](#delta-updates) bindings. This method of ingress writes rows directly to Snowflake tables and scales compute automatically based on load.

### High-performance Snowpipe Streaming

Snowflake's [high-performance Snowpipe Streaming architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
is available behind the `snowpipe_streaming_v2` [feature flag](/guides/advanced-usage/feature-flags). It uses Snowflake's official
streaming SDK, and rows are sent to Snowflake as your collection documents are materialized rather than being staged first.

To use it, all of the following must be true:

* The binding uses [delta updates](#delta-updates).
* The endpoint configuration uses [key-pair (JWT) authentication](#key-pair-authentication).
* `snowpipe_streaming_v2` is set in the endpoint configuration's `advanced.feature_flags`. It cannot be combined
  with the `snowpipe_streaming` flag, which selects the older write path.
* The task runs on Estuary's V2 materialization runtime, which is selected with the `enable-runtime-v2` shard flag:

  ```yaml
  materializations:
    acmeCo/snowflake-materialization:
      # ...
      shards:
        flags:
          enable-runtime-v2: "true"
  ```

A task that sets the feature flag without the runtime flag refuses to start. The publication itself succeeds — it
reports a warning rather than an error — so check the task after you publish.
Contact [Estuary support](mailto:support@estuary.dev) before enabling this write path.

Opting a binding into this write path is one-way. Once the binding has materialized rows through it, a change that
would move it back — removing the feature flag, changing the binding away from delta updates, or changing the
endpoint's authentication — publishes successfully but leaves the task unable to start, with an error naming the
binding. This is deliberate: the connector's record
of what Snowflake already holds does not survive a switch of write path, and a later return to this one would drop
rows silently. [Backfilling](/reference/backfilling-data/#materialization-backfill) the binding is the way off, and it
starts the binding on the new path with no such record to lose.

Moving a binding *onto* this write path is allowed at any time, and needs no backfill: the existing table is adopted,
and the rows already in it are left alone. There is one condition. If the task's checkpoint still holds work that the
previous write path staged and did not finish — rows it had written but not yet registered with Snowflake — the
publication succeeds and the task then refuses to start, naming the table and what is outstanding. Restore the write
path the task was running, let it commit one transaction to finish that work, then move the binding onto this path
again. Nothing is lost, and no backfill is needed. Backfilling the binding also clears it, at the cost of
materializing it again.

#### Delivery semantics

Because rows are sent as they are materialized instead of being staged and applied at the end of a transaction,
this write path has different delivery semantics than every other Snowflake write path:

* **Rows can become visible in the destination table slightly before the Estuary transaction that produced them commits.**
  A query run at exactly the wrong moment can therefore observe rows of a transaction that has not committed yet.
* **Rows of a transaction that is interrupted before it commits remain in the table.** Nothing removes them.
  When the interrupted transaction is retried, the connector establishes which rows Snowflake already holds and
  sends only the remainder, so the retry does not duplicate them.
* **Every transaction that commits is delivered exactly once**, including across task restarts and unclean shutdowns.

If the connector cannot establish which rows Snowflake already holds, it fails rather than risk duplicating or dropping
rows. This happens if the destination lost data the connector had already committed, or if the task was scaled to a
different number of shards while an interrupted transaction's rows were outstanding. In either case,
[backfill](/reference/backfilling-data/#materialization-backfill) the affected binding to recover: this materializes the
binding from the beginning and resets the connector's streaming state along with it.

A backfill on this write path drops the destination table and creates it again, rather than truncating it. Snowflake
binds a stream to the table it writes into, and a truncate would leave the outgoing streams valid and pointed at the
emptied table, so their next rows would survive the truncate and be materialized twice. Dropping the table is what
ends those streams with it. Two consequences follow:

* Grants and other object-level settings on the old table do not survive the backfill. Grant them to a role that
  Snowflake re-applies, or re-apply them afterwards.
* [`retain_existing_data_on_backfill`](/guides/advanced-usage/feature-flags#retain_existing_data_on_backfill) has no
  effect on a binding using this write path, because the table it would preserve data in no longer exists.

The connector also fails if Snowflake rejects a row outright — for example, a null value for a column the table
declares `NOT NULL`. Snowflake discards such a row without failing the write, and reports it only in a count of
rejected rows, which the connector checks as each transaction commits and again whenever it resumes writing to a
table. Because a discarded row cannot be identified after the fact, it cannot be re-sent, so this failure also holds
until you backfill the binding rather than letting a retry continue with the row missing. The connector marks a column
`NOT NULL` only for a field your collection schema requires, and the runtime always supplies those, so this should not
arise for a table the connector created and still manages.

:::caution
Streaming requires a destination that Snowflake can stream into — a table, not a view. Snowflake reports an
incompatible destination asynchronously, so the connector surfaces it as a failure to commit the transaction
(`ERR_PIPE_IN_INVALID_STATE`) a few seconds after the rows are sent, rather than as an error on a specific document.
:::

## Timestamp Data Type Mapping

The Snowflake materialization connector requires setting an expected timestamp type.
These types map to Snowflake's [timestamp data types](https://docs.snowflake.com/en/sql-reference/data-types-datetime#label-datatypes-timestamp-variations) with some caveats for `TIMESTAMP_NTZ`.

Available options in Estuary include:

* `TIMESTAMP_LTZ`

   Stored as a UTC point-in-time. Snowflake performs automatic timezone normalization.

* `TIMESTAMP_NTZ` (discard TZ)

   Stored as a wall-clock time without timezone. The source timezone is discarded.

   This `NTZ` variant is recommended for **existing** tasks that already use `TIMESTAMP_NTZ`.

   The type is written as `TIMESTAMP_NTZ_DISCARD` when working directly with the materialization specification.

* `TIMESTAMP_NTZ` (normalize to UTC)

   Stored as a wall-clock time without timezone. The connector normalizes to UTC prior to storage within Snowflake.

   This `NTZ` variant is recommended for **new** tasks that use `TIMESTAMP_NTZ`, as it aligns well with Snowflake's default behavior.

   The type is written as `TIMESTAMP_NTZ_NORMALIZE` when working directly with the materialization specification.

* `TIMESTAMP_TZ`

   Stored as a timestamp with associated timezone.

You do not need to explicitly set the [`TIMESTAMP_TYPE_MAPPING` configuration](https://docs.snowflake.com/en/sql-reference/parameters#timestamp-type-mapping) in Snowflake.
However, if you do, the value in Snowflake **must** match the value in Estuary.

## Reserved words

Snowflake has a list of reserved words that must be quoted in order to be used as an identifier. Estuary automatically quotes fields that are in the reserved words list. You can find this list in Snowflake's documentation [here](https://docs.snowflake.com/en/sql-reference/reserved-keywords.html) and in the table below.

:::caution
In Snowflake, objects created with quoted identifiers must always be referenced exactly as created, including the quotes. Otherwise, SQL statements and queries can result in errors. See the [Snowflake docs](https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#double-quoted-identifiers).
:::

|Reserved words| | |
|---|---|---|
| account	|from	|qualify|
|all|	full|	regexp|
|alter|	grant	|revoke|
|and|	group	|right|
|any|	gscluster	|rlike|
|as	|having	|row|
|between|	ilike	|rows|
|by	|in	|sample|
|case	|increment|	schema|
|cast	|inner|	select|
|check|	insert|	set|
|column	|intersect|	some|
|connect|	into|	start|
|connection|	is|	table|
|constraint	|issue|	tablesample|
|create	|join	|then|
|cross|	lateral	|to|
|current|	left|	trigger|
|current_date|	like|	true |
|current_time	|localtime|	try_cast|
|current_timestamp	|localtimestamp|	union|
|current_user|	minus|	unique|
|database	|natural	|update|
|delete	|not|	using|
|distinct	|null|	values|
|drop	|of	|view|
|else|	on|	when|
|exists	|or	|whenever |
|false |	order|	where|
|following|	organization|	with|
|for| | |
