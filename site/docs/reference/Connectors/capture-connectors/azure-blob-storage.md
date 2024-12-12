
# Azure Blob Storage

This connector captures data from an Azure Blob Storage Account.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-azure-blob-storage:dev`](https://ghcr.io/estuary/source-s3:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## **Prerequisites**

You will need the following values to authenticate to Azure and an active subscription

- **Subscription ID**
- **Client ID**
- **Client Secret**
- **Tenant ID**

## Setup **a Microsoft Entra application**

These values can be obtained from the portal, here's the instructions:

- Get Subscription ID
    1. Login into your Azure account
        1. Select [Subscriptions](https://portal.azure.com/#view/Microsoft_Azure_Billing/SubscriptionsBladeV2) in the left sidebar
    2. Select whichever subscription is needed
    3. Click on Overview
    4. Copy the Subscription ID
- Get Client ID / Client Secret / Tenant ID
    1. Go to Azure Active Directory, then select App registrations.
    2. Click New registration, fill out the required fields like Name and Supported account types.
    3. Click Register to create the new app registration.
    4. After registration, note down the Application (client) ID value.
    5. Go to Certificates & secrets and click New client secret. Fill in a Description, choose a Secret value type and length, then click Add to save the secret. A pop-up will appear with your new client secret value; copy it immediately as you won't be able to view it again.
    6. Go back to Overview page and copy the Directory (tenant) ID under Properties.
    7. Your service principal is now created, and you have its Application (client) ID, Directory (tenant) ID, and a client secret key. Use these values when configuring your application or service to interact with Azure services that require authentication through AAD.

## **Configuration**

You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Azure Blob Storage source connector.

### **Properties**

#### **Endpoint**

| Property                         | Title                 | Description                                                                                                                                                                                                                                                                                                                                                 | Type    | Required/Default                                        |
| -------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ------------------------------------------------------- |
| /advanced                        |                       | Options for advanced users. You should not typically need to modify these.                                                                                                                                                                                                                                                                                  | object  |                                                         |
| /advanced/ascendingKeys          | Ascending Keys        | Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. If data is not ordered correctly, using ascending keys could cause errors. | boolean | false                                                   |
| /credentials                     | Credentials           | Azure credentials used to authenticate with Azure Blob Storage.                                                                                                                                                                                                                                                                                             | object  |                                                         |
| /credentials/storageAccountName  | Storage Account Name  | The name of the Azure Blob Storage account.                                                                                                                                                                                                                                                                                                                 | string  | Required.                                               |
| /credentials/azureClientID       | Azure Client ID       | The client ID used to authenticate with Azure Blob Storage.                                                                                                                                                                                                                                                                                                 | string  | Required if using OAuth2                                |
| /credentials/azureClientSecret   | Azure Client Secret   | The client secret used to authenticate with Azure Blob Storage.                                                                                                                                                                                                                                                                                             | string  | Required if using OAuth2                                |
| /credentials/azureTenantID       | Azure Tenant ID       | The ID of the Azure tenant where the Azure Blob Storage account is located.                                                                                                                                                                                                                                                                                 | string  | Required if using OAuth2                                |
| /credentials/azureSubscriptionID | Azure Subscription ID | The ID of the Azure subscription that contains the Azure Blob Storage account.                                                                                                                                                                                                                                                                              | string  | Required if using OAuth2                                |
| /credentials/ConnectionString    | Connection String     | The connection string used to authenticate with Azure Blob Storage.                                                                                                                                                                                                                                                                                         | string  | Required if using the Connection String authentication. |
| /containerName                   | Container Name        | The name of the Azure Blob Storage container to read from.                                                                                                                                                                                                                                                                                                  | string  | Required.                                               |
| /matchKeys                       | Match Keys            | Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.                                                                                                                                                       | string  |                                                         |

#### **Bindings**

| Property | Title     | Title              | Type   | Required/Default |
| -------- | --------- | ------------------ | ------ | ---------------- |
| /stream  | Container | The container name | string | required         |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-azure-blob-storage:dev"
        config:
          containerName: example
          credentials:
            azureClientID: e2889d31-aaaa-bbbb-cccc-85bb5a33d7a5
            azureClientSecret: just-a-secret
            azureSubscriptionID: f1a5bc81-aaaa-bbbb-cccc-b926c154ecc7
            azureTenantID: d494a2c6-aaaa-bbbb-cccc-ef1e5eaa64a6
            storageAccountName: example
          parser:
            compression: zip
            format:
              type: csv
              config:
                delimiter: ","
                encoding: UTF-8
                errorThreshold: 5
                headers: [ID, username, first_name, last_name]
                lineEnding: "\\r"
                quote: "\""
    bindings:
      - resource:
          stream: example
        target: ${PREFIX}/${COLLECTION_NAME}
```

### **Advanced: Parsing cloud storage data**

Cloud storage platforms like Azure Blob Storage can support a wider variety of file types than other data source systems. For each of these file types, Flow must parse and translate data into collections with defined fields and JSON schemas.

By default, the parser will automatically detect the type and shape of the data in your bucket, so you won't need to change the parser configuration for most captures.

However, the automatic detection may be incorrect in some cases. To fix or prevent this, you can provide explicit information in the parser configuration, which is part of the endpoint configuration for this connector.

The parser configuration includes:

- **Compression**: Specify how the bucket contents are compressed.
If no compression type is specified, the connector will try to determine the compression type automatically.
Options are:
    - **zip**
    - **gzip**
    - **zstd**
    - **none**
- **Format**: Specify the data format, which determines how it will be parsed.
Options are:
    - **Auto**: If no format is specified, the connector will try to determine it automatically.
    - **Avro**
    - **CSV**
    - **JSON**
    - **Protobuf**
    - **W3C Extended Log**
- info

    At this time, Flow only supports S3 captures with data of a single file type.
    Support for multiple file types, which can be configured on a per-binding basis,
    will be added in the future.

    For now, use a prefix in the endpoint configuration to limit the scope of each capture to data of a single file type.


### CSV configuration

CSV files include several additional properties that are important to the parser.
In most cases, Flow is able to automatically determine the correct values,
but you may need to specify for unusual datasets. These properties are:

- **Delimiter**. Options are:
    - Comma (`","`)
    - Pipe (`"|"`)
    - Space (`"0x20"`)
    - Semicolon (`";"`)
    - Tab (`"0x09"`)
    - Vertical tab (`"0x0B"`)
    - Unit separator (`"0x1F"`)
    - SOH (`"0x01"`)
    - Auto
- **Encoding** type, specified by its [WHATWG label](https://encoding.spec.whatwg.org/#names-and-labels).
- Optionally, an **Error threshold**, as an acceptable
percentage of errors. If set to a number greater than zero, malformed
rows that fall within the threshold will be excluded from the capture.
- **Escape characters**. Options are:
    - Backslash (`"\\"`)
    - Disable escapes (`""`)
    - Auto
- Optionally, a list of column **Headers**, if not already included in the first row of the CSV file.

    If any headers are provided, it is assumed that the provided list of
    headers is complete and authoritative.
    The first row of your CSV file will be assumed to be data (not headers),
     and you must provide a header value for every column in the file.

- **Line ending** values
    - CRLF (`"\\r\\n"`) (Windows)
    - CR (`"\\r"`)
    - LF (`"\\n"`)
    - Record Separator (`"0x1E"`)
    - Auto
- **Quote character**
    - Double Quote (`"\""`)
    - Single Quote (`"`)
    - Disable Quoting (`""`)
    - Auto

The sample specification [above](https://docs.estuary.dev/reference/Connectors/capture-connectors/amazon-s3/#sample) includes these fields.
