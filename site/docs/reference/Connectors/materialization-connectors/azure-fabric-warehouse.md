
import ReactPlayer from "react-player";

# Microsoft Azure Fabric Warehouse

This connector materializes Flow collections into tables in Microsoft Azure
Fabric Warehouse.

[`ghcr.io/estuary/azure-fabric-warehouse:dev`](https://ghcr.io/estuary/azure-fabric-warehouse:dev)
provides the latest connector image. You can also follow the link in your
browser to see past image versions.

<ReactPlayer controls url="https://www.youtube.com/watch?v=_aPyCWLciDs" />

## Prerequisites

To use this connector, you'll need:
- The connection string for a [Fabric
  Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-warehouse).
  See
  [instructions](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity#retrieve-the-sql-connection-string)
  for finding the connection string.
- A service principal for connecting to the warehouse. The **Client ID** and
  **Client Secret** are needed to configure the connector.
  - Follow [this
    guide](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal)
    to register a Microsoft Entra app and create a service principal. Use
    **Option 3: Create a new client secret** to create the service principal and
    save its client secret.
  - Follow [these
    instructions](https://learn.microsoft.com/en-us/fabric/data-warehouse/entra-id-authentication#tenant-setting)
    for enabling service principal access to Fabric APIs.
  - Assign the service principal the **Contributor** role for the workspace as
    described
    [here](https://learn.microsoft.com/en-us/fabric/data-warehouse/entra-id-authentication#workspace-setting).
- A **Storage Account Key** for a storage account that will be used to store
  temporary staging files that will be loaded into your warehouse. Follow [this
  guide](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create)
  to create a storage account. You can find your storage account key using
  [these
  instructions](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys).
- The name of the container within the storage account for storing staging
  files.


## Configuration

Use the below properties to configure the materialization, which will direct one or more of your
Flow collections to your tables.

### Properties

#### Endpoint

| Property                  | Title                          | Description                                                                                                                                             | Type   | Required/Default |
|---------------------------|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/clientID`**           | Client ID                      | Client ID for the service principal used to connect to the Azure Fabric Warehouse. services.                                                            | string | Required         |
| **`/clientSecret`**       | Client Secret                  | Client Secret for the service principal used to connect to the Azure Fabric Warehouse. services.                                                        | string | Required         |
| **`/warehouse`**          | Warehouse                      | Name of the Azure Fabric Warehouse to connect to.                                                                                                   | string | Required         |
| **`/schema`**             | Schema                         | Schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables. | string | Required         |
| **`/connectionString`**   | Connection String              | SQL connection string for the Azure Fabric Warehouse.                                                                                                   | string | Required         |
| **`/storageAccountName`** | Storage Account Name           | Name of the storage account that temporary files will be written to.                                                                                    | string | Required         |
| **`/storageAccountKey`**  | Storage Account Key            | Storage account key for the storage account that temporary files will be written to.                                                                    | string | Required         |
| **`/containerName`**      | Storage Account Container Name | Name of the container in the storage account where temporary files will be written.                                                                     | string | Required         |
| `/directory`              | Directory                      | Optional prefix that will be used for temporary files.                                                                                                  | string |                  |

#### Bindings

| Property         | Title              | Description                                                | Type    | Required/Default |
|------------------|--------------------|------------------------------------------------------------|---------|------------------|
| **`/table`**     | Table              | Table name                                                 | string  | Required         |
| `/schema`        | Alternative Schema | Alternative schema for this table                          | string  |                  |
| `/delta_updates` | Delta updates      | Whether to use standard or [delta updates](#delta-updates) | boolean |                  |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-azure-fabric-warehouse:dev"
        config:
          clientID: <client-id>
          clientSecret: <client-secret>
          connectionString: <connection-string>
          containerName: storage-container
          hardDelete: false
          schema: main
          storageAccountKey: <storage-key>
          storageAccountName: storagename
          warehouse: estuary-wh
    bindings:
      - resource:
          table: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Sync Schedule

This connector supports configuring a schedule for sync frequency. You can read
about how to configure this [here](../../materialization-sync-schedule.md).

## Delta updates

This connector supports both standard (merge) and [delta
updates](../../../concepts/materialization.md#delta-updates). The default is to
use standard updates.

Enabling delta updates will prevent Flow from querying for documents in your
tables, which can reduce latency and costs for large datasets. If you're certain
that all events will have unique keys, enabling delta updates is a simple way to
improve performance with no effect on the output. However, enabling delta
updates is not suitable for all workflows, as the resulting table won't be fully
reduced.
