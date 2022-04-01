# Snowflake

This connector materializes Flow collections into tables in a Snowflake database.

TODO--  high-level details about credentials used, mechanism, etc, if there's anything the user needs to know

[`ghcr.io/estuary/materialize-snowflake:dev`](https://ghcr.io/estuary/materialize-snowflake:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Snowflake account that includes:
    * A target database, to which you'll materialize data
    * A [schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html) — a logical grouping of database objects — within the target database
    * A user with a role assigned that [grants the `MODIFY` privilege](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html) on the target database
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

TODO - probably. Pre-requisites seem potentially complex and we may need setup steps for this one.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Snowflake materialization, which will direct one or more of your Flow collections to new Snowflake tables.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/account`** | Account | The Snowflake account identifier | string | Required |
| **`/database`** | Database | Name of the Snowflake database to which to materialize | string | Required |
| **`/password`** | Password | Snowflake user password | string | Required |
| `/region` | Region | Region where the account is located | string |  |
| `/role` | Role | ???Why/how is this used??? Role assigned to the user | string |  |
| **`/schema`** | Schema | Snowflake schema within the database to which to materialize | string | Required |
| **`/user`** | Use | Snowflake username | string | Required |
| `/warehouse` | Warehouse | Name of the data warehouse that contains the database | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/delta_updates` | Delta updates | Whether to use standard or [delta updates](#delta-updates) | boolean |  |
| **`/table`** | Table | Table name | string | Required |

### Sample

```yaml

materializations:
  ${tenant}/${mat_name}:
	  endpoint:
  	    connector:
    	    config:
              account: acmeCo
              database: acmeCo_db
              password: secret
              region: us-east-1
              schema: acmeCo_flow_schema
              user: snowflake_user
              warehouse: acmeCo_warehouse
    	    image: ghcr.io/estuary/materialize-snowflake:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
    source: ${tenant}/${source_collection}
```

## Delta updates

TODO - verify. Stolen from BQ docs because I assume it's the same but might not be.

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
    source: ${tenant}/${source_collection}
```
