# Snowflake

This connector materializes Flow collections into tables in a Snowflake database.
It allows both standard and [delta updates](#delta-updates). [Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro) is additionally available for delta update bindings.

The connector first uploads data changes to a [Snowflake table stage](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html#table-stages).
From there, it transactionally applies the changes to the Snowflake table.

[`ghcr.io/estuary/materialize-snowflake:dev`](https://ghcr.io/estuary/materialize-snowflake:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Snowflake account that includes:
    * A target database, to which you'll materialize data
    * A [schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html) — a logical grouping of database objects — within the target database
    * A virtual warehouse
    * A user with a role assigned that grants the appropriate access levels to these resources.
    See the [script below](#setup) for details.
* Know your Snowflake account's host URL. This is formatted using your [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used),
for example, `orgname-accountname.snowflakecomputing.com`.
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

To meet the prerequisites, copy and paste the following script into the Snowflake SQL editor, replacing the variable names in the first six lines.

If you'd like to use an existing database, warehouse, and/or schema, be sure to set
`database_name`, `warehouse_name`, and `estuary_schema` accordingly. If you specify a new name, the script will create the item for you. You can set `estuary_role`, `estuary_user`, and `estuary_password` to whatever you'd like.

Check the **All Queries** check box, and click **Run**.

```sql
set database_name = 'ESTUARY_DB';
set warehouse_name = 'ESTUARY_WH';
set estuary_role = 'ESTUARY_ROLE';
set estuary_user = 'ESTUARY_USER';
set estuary_password = 'secret';
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
password = $estuary_password
default_role = $estuary_role
default_warehouse = $warehouse_name;
grant role identifier($estuary_role) to user identifier($estuary_user);
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

### Key-pair Authentication & Snowpipe

In order to enable use of Snowpipe for [delta updates](#delta-updates) bindings, you need to authenticate
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
ALTER USER $estuary_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...'
```

Verify the public key fingerprint in Snowflake matches the one you have locally:
```sql
DESC USER $estuary_user;
SELECT TRIM((SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "property" = 'RSA_PUBLIC_KEY_FP'), 'SHA256:');
```

Then compare with the local version:
```bash
openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```

Now you can use the generated _private key_ when configuring your Snowflake connector. Once you have key-pair authentication enabled, delta updates bindings will use Snowpipe for loading data.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Snowflake materialization, which will direct one or more of your Flow collections to new Snowflake tables.

### Properties

#### Endpoint

| Property                     | Title               | Description                                                                                                                                                     | Type   | Required/Default |
|------------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/account`**               | Account             | The Snowflake account identifier                                                                                                                                | string | Required         |
| **`/database`**              | Database            | Name of the Snowflake database to which to materialize                                                                                                          | string | Required         |
| **`/host`**                  | Host (Account URL)  | The Snowflake Host used for the connection. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol).                                  | string | Required         |
| `/role`                      | Role                | Role assigned to the user                                                                                                                                       | string |                  |
| **`/schema`**                | Schema              | Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables | string | Required         |
| `/warehouse`                 | Warehouse           | Name of the data warehouse that contains the database                                                                                                           | string |                  |
| `/account`                   | Account             | Optional Snowflake account identifier                                                                                                                           | string |                  |
| **`/credentials`**           | Credentials         | Credentials for authentication                                                                                                                                  | object | Required         |
| **`/credentials/auth_type`** | Authentication type | One of `user_password` or `jwt`                                                                                                                                 | string | Required         |
| **`/credentials/user`**      | User                | Snowflake username                                                                                                                                              | string | Required         |
| `/credentials/password`      | Password            | Required if using user_password authentication                                                                                                                  | string | Required         |
| `/credentials/privateKey`    | Private Key         | Required if using jwt authentication                                                                                                                            | string | Required         |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table name | string | Required |
| `/schema` | Alternative Schema | Alternative schema for this table | string |  |
| `/delta_updates` | Delta updates | Whether to use standard or [delta updates](#delta-updates) | boolean |  |

### Sample

User and password authentication:

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
  	    connector:
    	    config:
              account: acmeCo
              database: acmeCo_db
              host: orgname-accountname.snowflakecomputing.com
              schema: acmeCo_flow_schema
              warehouse: acmeCo_warehouse
              credentials:
                auth_type: user_pasword
                user: snowflake_user
                password: secret
    	    image: ghcr.io/estuary/materialize-snowflake:dev
  # If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
    source: ${PREFIX}/${source_collection}
```

Key-pair authentication:

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
  	    connector:
    	    config:
              account: acmeCo
              database: acmeCo_db
              host: orgname-accountname.snowflakecomputing.com
              schema: acmeCo_flow_schema
              warehouse: acmeCo_warehouse
              credentials:
                auth_type: jwt
                user: snowflake_user
                privateKey: |
                  -----BEGIN PRIVATE KEY-----
                  MIIEv....
                  ...
                  ...
                  ...
                  ...
                  ...
                  -----END PRIVATE KEY-----
    	    image: ghcr.io/estuary/materialize-snowflake:dev
  # If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
    source: ${PREFIX}/${source_collection}
```

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](../../materialization-sync-schedule.md).

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

This connector supports both standard (merge) and [delta updates](../../../concepts/materialization.md#delta-updates).
The default is to use standard updates.

Enabling delta updates will prevent Flow from querying for documents in your Snowflake table, which can reduce latency and costs for large datasets.
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

### Snowpipe

[Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro) allows for loading data into target tables without waking up the warehouse, which can be cheaper and more performant. Snowpipe can be used for delta updates bindings, and it requires configuring your authentication using a private key. Instructions for configuring key-pair authentication can be found in this page: [Key-pair Authentication & Snowpipe](#key-pair-authentication--snowpipe)

## Timestamp Data Type Mapping

Flow uses the `TIMESTAMP` type alias in Snowflake for materializing timestamp data types. This type alias points to either `TIMESTAMP_NTZ` (default), `TIMESTAMP_TZ` or `TIMESTAMP_LTZ`. The default `TIMESTAMP_NTZ` mapping means timestamps are normalised to UTC upon materialization. If you want to have timezone data as part of the timestamp, set the `TIMESTAMP_TYPE_MAPPING` configuration to `TIMESTAMP_TZ`. See [Snowflake documentation on `TIMESTAMP_TYPE_MAPPING` for more information](https://docs.snowflake.com/en/sql-reference/parameters#timestamp-type-mapping).

## Reserved words

Snowflake has a list of reserved words that must be quoted in order to be used as an identifier. Flow automatically quotes fields that are in the reserved words list. You can find this list in Snowflake's documentation [here](https://docs.snowflake.com/en/sql-reference/reserved-keywords.html) and in the table below.

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
