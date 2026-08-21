---
description: Use the commercetools connector to sync orders, customers, and payments, using OAuth2 client credentials from a commercetools API Client.
---

# commercetools

This connector captures data from [commercetools](https://docs.commercetools.com/api) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [orders](https://docs.commercetools.com/api/projects/orders) | Incremental |
| [customers](https://docs.commercetools.com/api/projects/customers) | Incremental |
| [payments](https://docs.commercetools.com/api/projects/payments) | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.


## Prerequisites

To set up the commercetools source connector, you'll need:

* Your commercetools **Project key**, and the **region** hosting that Project. Both are visible in the Merchant Center under **Settings > Developer settings**, in the API URL shown for your Project.
* A commercetools [API Client](https://docs.commercetools.com/api/authorization) created in the Merchant Center under **Settings > Developer settings > Create new API client**, with the following [scopes](https://docs.commercetools.com/api/scopes):
   * `view_orders`
   * `view_customers`
   * `view_payments`
   Note the client ID and secret when the client is created.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the commercetools source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/region`** | Region | The region hosting your commercetools Project. One of `us-central1.gcp`, `us-east-2.aws`, `europe-west1.gcp`, `eu-central-1.aws`, or `australia-southeast1.gcp`. | string | Required |
| **`/project_key`** | Project Key | The key of your commercetools Project. | string | Required |
| **`/credentials/client_id`** | Client ID | The API Client's client ID. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The API Client's client secret. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `OAuth Credentials`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data modified before this date will not be replicated. If left blank, all available data will be captured. | string | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | PT5M |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-commercetools:v1
        config:
          region: us-central1.gcp
          project_key: my-project
          credentials:
            credentials_title: OAuth Credentials
            client_id: <secret>
            client_secret: <secret>
          start_date: 2026-08-10T00:00:00Z
    bindings:
      - resource:
          name: orders
        target: ${PREFIX}/orders
      - resource:
          name: customers
        target: ${PREFIX}/customers
      - resource:
          name: payments
        target: ${PREFIX}/payments
```
