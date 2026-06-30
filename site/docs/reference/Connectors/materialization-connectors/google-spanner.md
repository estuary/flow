---
description: Materialize Estuary data collection into Google Spanner tables, with settings to configure hard deletes, custom creation SQL, and optimizations.
---

# Google Spanner

This connector materializes Estuary collections into tables in Google Spanner.

## Prerequisites

To use this connector, you'll need:

* A [Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#creating_a_project) with the Spanner API enabled.
* A Google Cloud Spanner instance and database.
* Credentials for a service account that can manage your Spanner instance.
* At least one Estuary collection to materialize.

## Configuration

To use this connector, begin with data in one or more Estuary collections.
Use the properties below to configure a Google Spanner materialization.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/project_id`** | Project ID | Name of the Google Cloud project containing your Spanner instance for this materialization. | string | Required |
| **`/instance_id`** | Instance ID | Cloud Spanner instance ID. | string | Required |
| **`/database`** | Database | Name of the Spanner database to use for the materialization. | string | Required |
| `/hardDelete` | Hard Delete | If this option is enabled, items deleted in the source will also be deleted from the destination. Otherwise, `_meta/op` will indicate soft deletes. | boolean | `false` |
| **`/credentials`** | Credentials | Credentials used to authenticate with Google. | object | Required |
| **`/credentials/service_account_json`** | Service Account JSON | The JSON key of the service account to use for authorization. | string | Required |
| `/advanced` | Advanced Options | Options for advanced users. | object |  |
| `/advanced/no_flow_document` | Exclude Flow Document | When enabled, the root document will not be required for standard updates. See [excluding flow_document with standard updates](/guides/customize-materialization-fields/#excluding-flow_document-with-standard-updates) for details. | boolean | `false` |
| `/advanced/disable_key_distribution_optimization` | Disable Key Distribution Optimization | When enabled, the hash prefix normally added to table keys will be omitted. The hash prefix distributes writes across Spanner splits and avoids hotspots. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table Name | Name of the table to publish materialized results to. | string | Required |
| `/schema` | Alternative Schema | Optional alternative schema for this table. Overrides the default namespace. | string |  |
| `/additional_table_create_sql` | Additional Table Create SQL | Additional SQL statement(s) to be run in the same transaction that creates the table. | string |  |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-spanner:v1
        config:
          project_id: my_google_cloud_project
          instance_id: my_spanner_instance
          database: my_db
          credentials:
            service_account_json: {secret}
    bindings:
      - resource:
          table: table_name
        source: ${PREFIX}/${source_collection}
```
