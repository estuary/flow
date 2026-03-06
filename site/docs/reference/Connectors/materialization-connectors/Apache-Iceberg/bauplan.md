# Bauplan

[Bauplan](https://www.bauplan.io) is a serverless data lake platform built natively on Apache Iceberg. It provides a managed REST catalog so you can run SQL and Python queries directly on your Iceberg tables without managing catalog infrastructure yourself.

This connector materializes Estuary collections into Bauplan as Iceberg tables. The connector is a variant of the [Apache Iceberg connector](./Apache-Iceberg.md). The setup steps are the same — refer to that page for the full configuration reference, including EMR Serverless setup. The only Bauplan-specific configuration is the catalog connection below.

:::tip
For a complete end-to-end setup guide, see the Bauplan documentation: **[Estuary via EMR](https://docs.bauplanlabs.com/integrations/data_int_and_etl/estuary)**
:::

## Catalog Configuration

Bauplan exposes a standard Iceberg REST catalog endpoint. When configuring the materialization, use the following:

- **Base URL**: Your Bauplan REST catalog URL (available from your Bauplan account)
- **Warehouse**: Your Bauplan warehouse name
- **Catalog Authentication**: Select **OAuth 2.0 Client Credentials** and supply the client ID and secret from your Bauplan account

For all other configuration options (EMR Serverless compute, staging bucket, IAM roles, bindings), refer to the [Apache Iceberg connector docs](./Apache-Iceberg.md).
