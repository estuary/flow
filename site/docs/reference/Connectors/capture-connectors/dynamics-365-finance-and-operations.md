# Microsoft Dynamics 365 Finance and Operations

This connector captures data from [Microsoft Dynamics 365 Finance and Operations](https://www.microsoft.com/en-us/dynamics-365) into Estuary collections. It does so by capturing data exported to Azure Data Lake Storage via [Azure Synapse Link for Dataverse](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data).

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-dynamics-365-finance-and-operations:dev`](https://ghcr.io/estuary/source-dynamics-365-finance-and-operations:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

This connector captures changes to Dynamics 365 Finance and Operations tables that are exported to Azure Data Lake Storage. All available tables will appear after authenticating and performing a discovery.

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

- A Microsoft Dynamics 365 Finance and Operations account
- An Azure Synapse Link that exports Finance and Operations data into Azure Data Lake Storage in CSV format. See Microsoft's [documentation](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data) for instructions on how to set up the exports with Azure Synapse Link.
- The Azure container and filesystem containing the exported data.

:::note
Azure Synapse Link supports exporting data in either CSV or Parquet format. This connector currently only works with CSVs, so ensure the CSV export option is chosen.

If you would like to capture data in Parquet format, please reach out to us with that request.
:::

### Authentication

Authentication with the Azure Data Lake Storage containing your Dynamics 365 Finance and Operations data is done with a Shared Access Signature (SAS) token. See Microsoft's [documentation](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) for instructions on how to create SAS tokens.

When creating the SAS token, ensure the following settings are configured:
 - Under the **Allowed services** section, choose **Blob**.
 - Under the **Allowed resource types** section, choose **Container** and **Object**.
 - Under the **Allowed permissions** section, choose **Read** and **List**.

![](<../connector-images/source-dynamics-365-finance-and-operations-sas-permissions.png>)

:::warning
SAS tokens have an expiration date and they cannot be renewed. When your SAS token expires, make sure to update your capture with a new SAS token.

:::

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Dynamics 365 Finance and Operations source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/account_name`** | Azure Account Name | The Azure account containing your Dynamics 365 Finance and Operations data. | string | Required |
| **`/filesystem`** | Filesystem | The filesystem containing your Dynamics 365 Finance and Operations data | string | Required |
| **`/credentials/sas_token`** | SAS Token | The SAS token for accessing the Azure Data Lake Storage containing your Dynamics 365 Finance and Operations data. | string | Required |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | PT15M  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-dynamics-365-finance-and-operations:dev
        config:
          account_name: my_account
          credentials:
            credentials_title: SAS Token
            sas_token: <secret>
          filesystem: my_filesystem
    bindings:
      - resource:
          name: accountingevent
        target: ${PREFIX}/accountingevent
```
