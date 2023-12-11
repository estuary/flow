# Databricks

This connector materializes Flow collections into tables in a Databricks SQL Warehouse.
It allows both standard and [delta updates](#delta-updates).

The connector first uploads data changes to a [Databricks Unity Catalog Volume](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html).
From there, it transactionally applies the changes to the Databricks tables.

[`ghcr.io/estuary/materialize-databricks:dev`](https://ghcr.io/estuary/materialize-databricks:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Databricks account that includes:
    * A unity catalog
    * A SQL Warehouse
    * A [schema](https://docs.databricks.com/api/workspace/schemas) — a logical grouping of tables in a catalog
    * A user with a role assigned that grants the appropriate access levels to these resources.
* At least one Flow collection

:::tip
If you haven't yet captured your data from its external source, start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md). You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

You need to first create a SQL Warehouse if you don't already have one in your account. See [Databricks documentation](https://docs.databricks.com/en/sql/admin/create-sql-warehouse.html) on configuring a Databricks SQL Warehouse. After creating a SQL Warehouse, you can find the details necessary for connecting to it under the **Connection Details** tab.

In order to save on costs, we recommend that you set the Auto Stop parameter for your SQL warehouse to the minimum available. Estuary's Databricks connector automatically delays updates to the destination up to a configured Update Delay (see the endpoint configuration below), with a default value of 30 minutes. If your SQL warehouse is configured to have an Auto Stop of more than 15 minutes, we disable the automatic delay since the delay is not as effective in saving costs with a long Auto Stop idle period.

You also need an access token for your user to be used by our connector, see the respective [documentation](https://docs.databricks.com/en/administration-guide/access-control/tokens.html) from Databricks on how to create an access token.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Databricks materialization, which will direct one or more of your Flow collections to new Databricks tables.

### Properties

#### Endpoint

| Property                                 | Title        | Description                                                                                                                       | Type                                                                                                               | Required/Default         |
|------------------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|--------------------------|
| **`/address`**                           | Address      | Host and port of the SQL warehouse (in the form of host[:port]). Port 443 is used as the default if no specific port is provided. | string                                                                                                             | Required                 |
| **`/http_path`**                         | HTTP Path    | HTTP path of your SQL warehouse                                                                                                   | string                                                                                                             | Required                 |
| **`/catalog_name`**                      | Catalog Name | Name of your Unity Catalog                                                                                                        | string                                                                                                             | Required                 |
| **`/schema_name`**                       | Schema Name  | Default schema to materialize to                                                                                                  | string                                                                                                             | `default` schema is used |
| **`/credentials`**                       | Credentials  | Authentication credentials                                                                                                        | object                                                                                                             |                          |
| **`/credentials/auth_type`**             | Role         | Authentication type, set to `PAT` for personal access token                                                                       | string                                                                                                             | Required                 |
| **`/credentials/personal_access_token`** | Role         | Personal Access Token                                                                                                             | string                                                                                                             | Required                 |
| /advanced                                | Advanced     | Options for advanced users. You should not typically need to modify these.                                                        | object                                                                                                             |                          |
| /advanced/updateDelay                    | Update Delay | Potentially reduce active warehouse time by increasing the delay between updates. Defaults to 30 minutes if unset.                | string                                                                                                             | 30m                      |

#### Bindings

| Property         | Title              | Description                                                | Type    | Required/Default |
|------------------|--------------------|------------------------------------------------------------|---------|------------------|
| **`/table`**     | Table              | Table name                                                 | string  | Required         |
| `/schema`        | Alternative Schema | Alternative schema for this table                          | string  | Required         |
| `/delta_updates` | Delta updates      | Whether to use standard or [delta updates](#delta-updates) | boolean | `false`          |

### Sample

```yaml

materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
  	    connector:
    	    config:
              address: dbc-abcdefgh-a12b.cloud.databricks.com
              catalog_name: main
              http_path: /sql/1.0/warehouses/abcd123efgh4567
              schema_name: default
              credentials:
                auth_type: PAT
                personal_access_token: secret
    	    image: ghcr.io/estuary/materialize-databricks:dev
  # If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
  	- resource:
      	table: ${table_name}
        schema: default
    source: ${PREFIX}/${source_collection}
```

## Delta updates

This connector supports both standard (merge) and [delta updates](../../../concepts/materialization.md#delta-updates).
The default is to use standard updates.

Enabling delta updates will prevent Flow from querying for documents in your Databricks table, which can reduce latency and costs for large datasets.
If you're certain that all events will have unique keys, enabling delta updates is a simple way to improve
performance with no effect on the output.
However, enabling delta updates is not suitable for all workflows, as the resulting table in Databricks won't be fully reduced.

You can enable delta updates on a per-binding basis:

```yaml
    bindings:
  	- resource:
      	table: ${table_name}
        schema: default
        delta_updates: true
    source: ${PREFIX}/${source_collection}
```
## Reserved words

Databricks has a list of reserved words that must be quoted in order to be used as an identifier. Flow automatically quotes fields that are in the reserved words list. You can find this list in Databricks's documentation [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-reserved-words.html) and in the table below.

:::caution
In Databricks, objects created with quoted identifiers must always be referenced exactly as created, including the quotes. Otherwise, SQL statements and queries can result in errors. See the [Databricks docs](https://docs.databricks.com/en/sql-reference/identifiers-syntax.html#double-quoted-identifiers).
:::

| Reserved words |               |
|----------------|---------------|
| ANTI           |               |
| EXCEPT         | FULL          |
| INNER          | INTERSECT     |
| JOIN           | LATERAL       |
| LEFT           | MINUS         |
| NATURAL        | ON            |
| RIGHT          | SEMI          |
| SEMI           | USING         |
| NULL           | DEFAULT       |
| TRUE           | FALSE         |
| CROSS          |               |
