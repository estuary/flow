# Amazon Redshift

This connector materializes Flow collections into tables in an Amazon Redshift database.

The connector uses your AWS account to materialize to Redshift tables by way of files in an S3
bucket. The files in the bucket as as a temporary staging area for data storage and retrieval.

[`ghcr.io/estuary/materialize-redshift:dev`](https://ghcr.io/estuary/materialize-redshift:dev)
provides the latest connector image. You can also follow the link in your browser to see past image
versions.

## Prerequisites

To use this connector, you'll need:

- A Redshift cluster accessible either directly or using an SSH tunnel. The user configured to
  connect to Redshift must have at least "create table" permissions for the configured schema. The
  connector will create new tables in the database per your specification. Tables created manually
  in advance are not supported. See [setup](#setup) for more information.
- An S3 bucket for staging temporary files. For best performance the bucket should be in the same
  region as your Redshift cluster. See [this
  guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) for
  instructions on setting up a new S3 bucket.
- An AWS root or IAM user with [read and write
  access](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)
  to the S3 bucket. For this user, you'll need the **access key** and **secret access key**. See the
  [AWS blog](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/) for help finding
  these credentials.

## Configuration

Use the below properties to configure an Amazon Redshift materialization, which will direct one or
more of your Flow collections to your desired tables in the database.

### Properties

#### Endpoint

| Property                  | Title             | Description                                                                                                                                                      | Type   | Required/Default |
| ------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/address`**            | Address           | Host and port of the database. Example: red-shift-cluster-name.account.us-east-2.redshift.amazonaws.com:5439                                                     | string | Required         |
| **`/user`**               | User              | Database user to connect as.                                                                                                                                     | string | Required         |
| **`/password`**           | Password          | Password for the specified database user.                                                                                                                        | string | Required         |
| `/database`               | Database          | Name of the logical database to materialize to. The materialization will attempt to connect to the default database for the provided user if omitted.            | string |                  |
| `/schema`                 | Database Schema   | Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables. | string | `"public"`       |
| **`/awsAccessKeyId`**     | Access Key ID     | AWS Access Key ID for reading and writing data to the S3 staging bucket.                                                                                         | string | Required         |
| **`/awsSecretAccessKey`** | Secret Access Key | AWS Secret Access Key for reading and writing data to the S3 staging bucket.                                                                                     | string | Required         |
| **`/bucket`**             | S3 Staging Bucket | Name of the S3 bucket to use for staging data loads.                                                                                                             | string | Required         |
| **`/region`**             | Region            | Region of the S3 staging bucket. For optimal performance this should be in the same region as the Redshift database cluster.                                     | string | Required         |
| `/bucketPath`             | Bucket Path       | A prefix that will be used to store objects in S3.                                                                                                               | string |                  |

#### Bindings

| Property         | Title              | Description                                                               | Type    | Required/Default |
| ---------------- | ------------------ | ------------------------------------------------------------------------- | ------- | ---------------- |
| **`/table`**     | Table              | Name of the database table.                                               | string  | Required         |
| `/delta_updates` | Delta Update       | Should updates to this table be done via delta updates. Default is false. | boolean | `false`          |
| `/schema`        | Alternative Schema | Alternative schema for this table (optional).                             | string  |                  |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-redshift:dev"
        config:
          address: "redshift-cluster.account.us-east-2.redshift.amazonaws.com:5439"
          user: user
          password: password
          database: db
          awsAccessKeyId: access_key_id
          awsSecretAccessKey: secret_access_key
          bucket: my-bucket
          region: us-east-2
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](/reference/materialization-sync-schedule).

## Setup

You must configure your cluster to allow connections from Estuary. This can be accomplished by
making your cluster accessible over the internet for the
[Estuary Flow IP addresses](/reference/allow-ip-addresses), or using an SSH tunnel. Connecting to the S3
staging bucket does not use the network tunnel and connects over HTTPS only.

Instructions for making a cluster accessible over the internet can be found
[here](https://aws.amazon.com/premiumsupport/knowledge-center/redshift-cluster-private-public/).
When using this option, database connections are made over SSL only.

For allowing secure connections via SSH tunneling:

1. Refer to the [guide](../../../../guides/connect-network/) to configure an SSH server on using an
   AWS EC2 instance.

2. Configure your connector as described in the [configuration](#configuration) section above, with
   the additional of the `networkTunnel` stanza to enable the SSH tunnel, if using. See [Connecting to
   endpoints on secure
   networks](../../../../concepts/connectors/#connecting-to-endpoints-on-secure-networks) for additional
   details and a sample.

## Naming Conventions

Redshift has requirements for [names and
identifiers](https://docs.aws.amazon.com/redshift/latest/dg/r_names.html) and this connector will
automatically apply quoting when needed. All table identifiers and column identifiers (corresponding
to Flow collection fields) are treated as lowercase, unless the
[enable_case_sensitive_identifier](https://docs.aws.amazon.com/redshift/latest/dg/r_enable_case_sensitive_identifier.html)
configuration is enabled on the cluster being materialized to. Table names for bindings must be
unique on a case-insensitive basis, as well as field names of the source collection. If any names
are not unique on a case-insensitive basis (ex: `myField` vs. `MyField`) the materialization will
fail to apply.

If necessary, you can add [projections](../../../concepts/advanced/projections.md) to your
collection specification to change field names.

## Performance considerations

For best performance there should at most one Redshift materialization active per Redshift schema.
Additional collections to be materialized should be added as bindings to this single materialization
rather than creating a separate materialization for each collection.

In order to achieve exactly-once processing of collection documents, the materialization creates and
uses metadata tables located in the schema configured by the endpoint `schema` property. To commit a
transaction, a table-level lock is acquired on these metadata tables. If there are multiple
materializations using the same metadata tables, they will need to take turns acquiring these locks.
This locking behavior prevents "serializable isolation violation" errors in the case of multiple
materializations sharing the same metadata tables at the expense of allowing only a single
materialization to be actively committing a transaction.

## Maximum record size

The maximum size of a single input document is 4 MB. Attempting to materialize collections with
documents larger than 4 MB will result in an error. To materialize this data you can use a
[derivation](../../../concepts/derivations.md) to create a derived collection with smaller
documents, or exclude fields containing excessive amounts of data by [customizing the materialized
fields](/guides/customize-materialization-fields/#field-selection-for-materializations).

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.
