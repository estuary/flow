# Snowflake

This connector materializes Flow collections into tables in a Snowflake database.
It allows both standard and [delta updates](#delta-updates).

The connector first uploads data changes to a [Snowflake table stage](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html#table-stages).
From there, it transactionally applies the changes to the Snowflake table.

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
| `/role` | Role | Role assigned to the user | string |  |
| **`/schema`** | Schema | Snowflake schema within the database to which to materialize | string | Required |
| **`/user`** | User | Snowflake username | string | Required |
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

## Reserved Words

Snowflake has a list of reserved words that must be quoted in order to be used as an identifier. Flow automatically quotes fields that are in the reserved words list. You can find this list in Snowflake's documentation [here](https://docs.snowflake.com/en/sql-reference/reserved-keywords.html) as well as [here](https://go.estuary.dev/a9vE7a) in the connector source code.
