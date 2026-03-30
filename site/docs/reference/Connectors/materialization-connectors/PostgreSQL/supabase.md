
# Supabase

This connector materializes Estuary collections into tables in a Supabase PostgreSQL database.

## Prerequisites

To use this connector, you'll need:

- A Postgres database in Supabase to materialize into, with user credentials.
  The connector will create new tables in the database per your specification. Tables created manually in advance are not supported.
- A Supabase direct connection hostname which bypasses the Supabase connection pooler.
  See [Setup](#setup) for details.
- At least one Estuary collection

## Setup

You must configure your database to allow connections from Estuary.

By default, Supabase guides users into connecting to their database through a
[Connection Pooler](https://supabase.com/docs/guides/database/connecting-to-postgres#connection-pooler).
Connection poolers are helpful for many applications, but unfortunately the pooler
does not work well with temporary tables and prepared statements that this connector relies on.

This materialization connector requires a direct connection address for your database.
The address can be found by clicking on the **Connect** button in your Supabase dashboard.
Under the **Direct connection** option, copy the host and port for your database.

For example, if the provided connection string is `postgresql://postgres:[YOUR-PASSWORD]@db.abcdef.supabase.co:5432/postgres`, then the address is `db.abcdef.supabase.co:5432`.

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the below properties to configure a Supabase materialization, which will direct one or more of your Estuary collections to your desired tables, or views, in the database.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/address`** | Address | Host and port of the database. If only the host is specified, port will default to `5432`. | string | Required |
| **`/user`** | User | Database user to connect as. | string | Required |
| `/database` | Database | Name of the logical database to materialize to. | string |   |
| `/schema` | Database Schema | Database [schema](https://www.postgresql.org/docs/current/ddl-schemas.html) to use for materialized tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables | string | `"public"` |
| `/hardDelete` | Hard Delete | If this option is enabled, items deleted in the source will also be deleted from the destination. Otherwise, `_meta/op` in the destination will signify whether rows have been deleted (soft-delete). | boolean | `false` |
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |   |
| `/advanced/sslmode` | SSL Mode | Overrides SSL connection behavior by setting the 'sslmode' parameter. | string |   |

##### Authentication

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/credentials`** | Authentication | Authentication method and credentials that provide access to the database. | object | Required |
| `/credentials/auth_type` | Auth Type | The authentication method to use. One of `UserPassword`, `AWSIAM`, `GCPIAM`, or `AzureIAM`. | string |  |
| `/credentials/password` | Password | Password for the specified database user. | string | Required for `UserPassword` auth |
| `/credentials/aws_region` | AWS Region | AWS region of your resource. | string | Required for `AWSIAM` auth |
| `/credentials/aws_role_arn` | AWS Role ARN | AWS role for Estuary to use that has access to the resource. | string | Required for `AWSIAM` auth |
| `/credentials/gcp_service_account_to_impersonate` | GCP Service Account | GCP service account email for Cloud SQL IAM authentication. | string | Required for `GCPIAM` auth |
| `/credentials/gcp_workload_identity_pool_audience` | Workload Identity Pool Audience | GCP workload identity pool audience. The format should be similar to: `//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider`. | string | Required for `GCPIAM` auth |
| `/credentials/azure_client_id` | Azure Client ID | Azure App Registration Client ID for Azure Active Directory authentication. | string | Required for `AzureIAM` auth |
| `/credentials/azure_tenant_id` | Azure Tenant ID | Azure Tenant ID for Azure Active Directory authentication. | string | Required for `AzureIAM` auth |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/additional_table_create_sql` | Additional Table Create SQL | Additional SQL statement(s) to be run in the same transaction that creates the table. | string  |   |
| `/delta_updates` | Delta Update | Should updates to this table be done via delta updates. | boolean | `false` |
| `/schema` | Alternative Schema | Alternative schema for this table (optional). Overrides schema set in endpoint configuration. | string |   |
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-supabase-postgres:v5
        config:
          database: flow
          address: db.abcdef.supabase.co:5432
          user: flow
          credentials:
            auth_type: UserPassword
            password: <secret>
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector supports both standard (merge) and [delta updates](/concepts/materialization/#delta-updates).
The default is to use standard updates.

## Reserved words

PostgreSQL has a list of reserved words that must be quoted in order to be used as an identifier.
Estuary considers all the reserved words that are marked as "reserved" in any of the columns in the official [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-keywords-appendix.html).

Estuary automatically quotes fields that are in this reserved words list.
