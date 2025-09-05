# Starburst

This connector transactionally materializes Flow collections into Iceberg or Delta Lake tables using AWS S3 storage in [Starburst Galaxy](https://www.starburst.io/platform/starburst-galaxy/).
The Starburst Galaxy connector supports only standard (merge) updates.

The connector makes use of S3 AWS storage for temporarily storing data during the materialization process.

[`ghcr.io/estuary/materialize-starburst:dev`](https://ghcr.io/estuary/materialize-starburst:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Starburst Galaxy account (To create one see: [Staburst Galaxy start](https://www.starburst.io/platform/starburst-galaxy/start/)) that includes:
  * A running cluster containing an [Amazon S3](https://docs.starburst.io/starburst-galaxy/working-with-data/create-catalogs/object-storage/s3.html) catalog
  * A [schema](https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/index.html#create-schema) which is a logical grouping of tables
  * Storage on S3 for temporary data with `awsAccessKeyId` and `awsSecretAccessKey` which should correspond to the chosen catalog
  * A user with a role assigned that grants access to create, modify, and drop tables in the specified Amazon S3 catalog
* At least one Flow collection

### Setup

To get the host, go to your Cluster -> Connection info -> Other clients, as specified in Starburst's [Connect clients](https://docs.starburst.io/starburst-galaxy/working-with-data/query-data/connect-clients.html) docs.

You will also need to grant access to temporary storage (Roles and privileges -> Select specific role -> Privileges -> Add privilege -> Location). "Create schema and table in location" should be selected. See the Starburst [docs](https://docs.starburst.io/starburst-galaxy/cluster-administration/manage-cluster-access/manage-users-roles-and-tags/account-and-cluster-privileges-and-entities.html#location-privileges-) for more.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Starburst materialization, which will direct one or more of your Flow collections to new Starburst tables.

### Properties

#### Endpoint

| Property                  | Title                  | Description                                                                                                        | Type   | Required/Default |
|---------------------------|------------------------|--------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/host`**               | Host and optional port |                                                                                                                    | string | Required         |
| **`/catalog`**            | Catalog Name           | Galaxy catalog Catalog                                                                                             | string | Required         |
| **`/schema`**             | Schema Name            | Default schema to materialize to                                                                                   | string | Required         |
| **`/account`**            | Account                | Galaxy account name                                                                                                | string | Required         |
| **`/password`**           | Password               | Galaxy account password                                                                                            | string | Required         |
| **`/awsAccessKeyId`**     | AWS Access Key ID      |                                                                                                                    | string | Required         |
| **`/awsSecretAccessKey`** | AWS Secret Access Key  |                                                                                                                    | string | Required         |
| **`/region`**             | AWS Region             | Region of AWS storage                                                                                              | string | Required         |
| **`/bucket`**             | Bucket name            |                                                                                                                    | string | Required         |
| **`/bucketPath`**         | Bucket path            | A prefix that will be used to store objects in S3.                                                                 | string | Required         |

#### Bindings

| Property     | Title              | Description                       | Type   | Required/Default |
|--------------|--------------------|-----------------------------------|--------|------------------|
| **`/table`** | Table              | Table name                        | string | Required         |
| `/schema`    | Alternative Schema | Alternative schema for this table | string |                  |

### Sample

```yaml

materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
  	    connector:
    	    config:
              host: HOST:PORT
              account: ACCOUNT
              password: PASSWORD
              catalog: CATALOG_NAME
              schema: SCHEMA_NAME
              awsAccessKeyId: AWS_ACCESS_KEY_ID
              awsSecretAccessKey: AWS_SECRET_KEY_ID
              region: REGION
              bucket: BUCKET
              bucketPath: BUCKET_PATH
    	    image: ghcr.io/estuary/materialize-starburst:dev
  # If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
      - resource:
          table: ${table_name}
          schema: default
    source: ${PREFIX}/${source_collection}
```

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](/reference/materialization-sync-schedule).

## Reserved words

Starburst Galaxy has a list of reserved words that must be quoted in order to be used as an identifier. Flow automatically quotes fields that are in the reserved words list. You can find this list in Trino's documentation [here](https://trino.io/docs/current/language/reserved.html) and in the table below.

:::caution
In Starburst Galaxy, objects created with quoted identifiers must always be referenced exactly as created, including the quotes. Otherwise, SQL statements and queries can result in errors. See the [Trino docs](https://trino.io/docs/current/language/reserved.html#language-identifiers).
:::

| Reserved words    |                 |         |
|-------------------|-----------------|---------|
| CUBE              | 	INSERT         | TABLE   |
| CURRENT_CATALOG   | 	INTERSECT      | THEN    |
| CURRENT_DATE      | 	INTO           | TRIM    |
| CURRENT_PATH      | 	IS             | TRUE    |
| CURRENT_ROLE      | 	JOIN           | UESCAPE |
| CURRENT_SCHEMA    | 	JSON_ARRAY     | UNION   |
| CURRENT_TIME      | 	JSON_EXISTS    | UNNEST  |
| CURRENT_TIMESTAMP | 	JSON_OBJECT    | USING   |
| CURRENT_USER      | 	JSON_QUERY     | VALUES  |
| DEALLOCATE        | 	JSON_TABLE     | WHEN    |
| DELETE            | 	JSON_VALUE     | WHERE   |
| DESCRIBE          | 	LEFT           | WITH    |
| DISTINCT          | 	LIKE           |         |
| DROP              | 	LISTAGG        |         |
| ELSE              | 	LOCALTIME      |         |
| END               | 	LOCALTIMESTAMP |         |
| ESCAPE            | 	NATURAL        |         |
| EXCEPT            | 	NORMALIZE      |         |
| EXECUTE           | 	NOT            |         |
| EXISTS            | 	NULL           |         |
