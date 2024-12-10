

# Firebolt

This Flow connector materializes [delta updates](../../../concepts/materialization.md#delta-updates) of Flow collections into Firebolt `FACT` or `DIMENSION` tables.

To interface between Flow and Firebolt, the connector uses Firebolt's method for [loading data](https://docs.firebolt.io/Guides/loading-data/loading-data.html):
First, it stores data as JSON documents in an S3 bucket.
It then references the S3 bucket to create a [Firebolt _external table_](https://docs.firebolt.io/Guides/loading-data/working-with-external-tables.html),
which acts as a SQL interface between the JSON documents and the destination table in Firebolt.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/materialize-firebolt:dev`](https://ghcr.io/estuary/materialize-firebolt:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* A Firebolt database with at least one [engine](https://docs.firebolt.io/Overview/engine-fundamentals.html#firebolt-engines)
* An S3 bucket where JSON documents will be stored prior to loading
  * The bucket must be in a [supported AWS region](https://docs.firebolt.io/Reference/available-regions.html) matching your Firebolt database.
  * The bucket may be public, or may be accessible by an IAM user. To configure your IAM user, see the [steps below](#setup).
* At least one Flow [collection](../../../concepts/collections.md)

:::tip
 If you haven't yet captured your data from its external source,
 start at the beginning of the [guide to create a dataflow](../../../guides/create-dataflow.md).
 You'll be referred back to this connector-specific documentation at the appropriate steps.
:::

### Setup

For non-public buckets, you'll need to configure access in AWS IAM.

1. Follow the [Firebolt documentation](https://docs.firebolt.io/Guides/loading-data/creating-access-keys-aws.html) to set up an IAM policy and role, and add it to the external table definition.

2. Create a new [IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console). During setup:

   1. Choose **Programmatic** (access key) access. This ensures that an **access key ID** and **secret access key** are generated. You'll use these to configure the connector.

   2. On the **Permissions** page, choose **Attach existing policies directly** and attach the policy you created in step 1.

3. After creating the user, download the IAM credentials file.
Take note of the **access key ID** and **secret access key** and use them  to configure the connector.
See the [Amazon docs](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html) if you lose your credentials.

## Configuration

To use this connector, begin with data in one or more Flow collections.
Use the below properties to configure a Firebolt materialization, which will direct Flow data to your desired Firebolt tables via an external table.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/aws_key_id` | AWS key ID | AWS access key ID for accessing the S3 bucket. | string |  |
| `/aws_region` | AWS region | AWS region the bucket is in. | string |  |
| `/aws_secret_key` | AWS secret access key | AWS secret key for accessing the S3 bucket. | string |  |
| **`/database`** | Database | Name of the Firebolt database. | string | Required |
| **`/engine_name`** | Engine Name | Name of the Firebolt engine to run your queries. | string | Required |
| **`/client_secret`** | Client Secret | Secret of your Firebolt service account. | string | Required |
| **`/s3_bucket`** | S3 bucket | Name of S3 bucket where the intermediate files for external table will be stored. | string | Required |
| `/s3_prefix` | S3 prefix | A prefix for files stored in the bucket. | string |  |
| **`/client_id`** | Client ID | ID of your Firebolt service account. | string | Required |
| **`/account_name`** | Account Name | Name of your [account](https://docs.firebolt.io/Overview/organizations-accounts.html) within your Firebolt organization. | string | Required |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Name of the Firebolt table to store materialized results in. The external table will be named after this table with an `_external` suffix. | string | Required |
| **`/table_type`** | Table type | Type of the Firebolt table to store materialized results in. See the [Firebolt docs](https://docs.firebolt.io/working-with-tables.html) for more details. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
	  endpoint:
        connector:
          config:
            database: my-db
            engine_name: my-engine-name
            client_secret: secret
            # For public S3 buckets, only the bucket name is required
            s3_bucket: my-bucket
            client_id: firebolt-user
            account_name: my-account
          # Path to the latest version of the connector, provided as a Docker image
          image: ghcr.io/estuary/materialize-firebolt:dev
	# If you have multiple collections you need to materialize, add a binding for each one
    # to ensure complete data flow-through
    bindings:
      - resource:
          table: table-name
          table_type: fact
      source: ${PREFIX}/${source_collection}
```

## Delta updates

Firebolt is an insert-only system; it doesn't support updates or deletes.
Because of this, the Firebolt connector operates only in [delta updates](../../../concepts/materialization.md#delta-updates) mode.
Firebolt stores all deltas — the unmerged collection documents — directly.

In some cases, this will affect how materialized views look in Firebolt compared to other systems that use standard updates.

## Reserved words

Firebolt has a list of reserved words, which my not be used in identifiers.
Collections with field names that include a reserved word will automatically be quoted as part of a Firebolt materialization.

|Reserved words| | |
|---|---|---|
| all |	false |	or |
| alter |	fetch |	order |
| and |	first |	outer |
| array |	float |	over |
| between |	from |	partition |
| bigint |	full |	precision |
| bool |	generate |	prepare |
| boolean |	group |	primary |
| both |	having |	quarter |
| case |	if |	right |
| cast |	ilike |	row |
| char |	in |	rows |
| concat |	inner |	sample |
| copy |	insert |	select |
| create |	int |	set |
| cross |	integer |	show |
| current_date |	intersect |	text |
| current_timestamp |	interval |	time |
| database |	is |	timestamp |
| date |	isnull |	top |
| datetime |	join |	trailing |
| decimal |	join_type |	trim |
| delete |	leading |	true |
| describe |	left |	truncate |
| distinct |	like |	union |
| double |	limit |	unknown_char |
| doublecolon |	limit_distinct |	unnest |
| dow |	localtimestamp |	unterminated_string |
| doy |	long |	update |
| drop |	natural |	using |
| empty_identifier |	next |	varchar |
| epoch |	not |	week |
| except |	null |	when |
| execute |	numeric |	where |
| exists |	offset |	with |
| explain |	on | |
| extract |	only | |
