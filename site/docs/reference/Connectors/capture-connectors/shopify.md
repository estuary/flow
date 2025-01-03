
# Shopify

This connector captures data from [Shopify's REST Admin API](https://shopify.dev/docs/api/admin-rest).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-shopify:dev`](https://ghcr.io/estuary/source-shopify:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

Alternatively, if you would like to receive Shopify webhooks directly in Estuary Flow, see the [HTTP Ingest (Webhook)](https://docs.estuary.dev/reference/Connectors/capture-connectors/http-ingest/) connector.

## Supported data resources

The following data resources are supported through the Shopify APIs:

### Default Streams

* [Abandoned Checkouts](https://shopify.dev/api/admin-rest/2023-10/resources/abandoned-checkouts)
* [Collects](https://shopify.dev/api/admin-rest/2023-10/resources/collect)
* [Custom Collections](https://shopify.dev/api/admin-rest/2023-10/resources/customcollection)
* [Customers](https://shopify.dev/api/admin-rest/2023-10/resources/customer)
* [Inventory Item](https://shopify.dev/api/admin-rest/2023-10/resources/inventoryitem)
* [Inventory Levels](https://shopify.dev/api/admin-rest/2023-10/resources/inventorylevel)
* [Locations](https://shopify.dev/api/admin-rest/2023-10/resources/location)
* [Metafields](https://shopify.dev/api/admin-rest/2023-10/resources/metafield)
* [Orders](https://shopify.dev/api/admin-rest/2023-10/resources/order)
* [Products](https://shopify.dev/api/admin-rest/2023-10/resources/product)
* [Transactions](https://shopify.dev/api/admin-rest/2023-10/resources/transaction)

### Shopify Plus Streams

* [User](https://shopify.dev/api/admin-rest/2023-10/resources/user#resource-object)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* Store ID of your Shopify account.

   Use the prefix of your admin URL. For example, `https://{store_id}.myshopify.com/admin`.

You can authenticate your account either via OAuth or using a Shopify [access token](https://shopify.dev/docs/api/usage/authentication).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Shopify source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials |  | object | Required |
| `/credentials/auth_type` | Authentication Type | Can either be `oauth` or `access_token`. | string | Required |
| `/credentials/client_id` | Client ID | The Client ID for Shopify OAuth. | string | Required when using the `oauth` Auth Type |
| `/credentials/client_secret` | Client Secret | The Client Secret for Shopify OAuth. | string | Required when using the `oauth` Auth Type |
| `/credentials/access_token` | Access Token | The access token to authenticate with the Shopify API. | string | Required |
| `/store` | Store ID | Shopify Store ID, such as from the prefix in `https://{store_id}.myshopify.com/admin`. | string | Required |
| `/start_date` | Start Date | UTC date in the format 2020-01-01. Any data before this date will not be replicated. | string | Required, `2020-01-01` |
| `/admin_url` | Admin URL | The Admin URL for the Shopify store (overrides 'store' property). | string |  |
| `/is_plus_account` | Is Plus Account | Enables Shopify plus account endpoints. | boolean |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Shopify project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-shopify:dev
        config:
          credentials:
            auth_type: access_token
            access_token: <secret>
          store: <store ID>
          is_plus_account: false
          start_date: 2020-01-01
    bindings:
      - resource:
          stream: transactions
          syncMode: full_refresh
        target: ${PREFIX}/transactions
      {...}
```
