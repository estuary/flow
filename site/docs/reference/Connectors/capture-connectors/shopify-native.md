
# Shopify (GraphQL)

This connector captures data from [Shopify's GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-shopify-native:dev`](https://ghcr.io/estuary/source-shopify-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Shopify API:

* [Abandoned Checkouts](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/abandonedCheckouts?example=Retrieves+a+list+of+abandoned+checkouts)
* [Custom Collections](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/collections?example=Retrieves+a+list+of+custom+collections)
   * Custom Collection Metafields
* [Customers](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/customers)
   * Customer Metafields
* [Fulfillment Orders](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/fulfillmentorders)
* [Fulfillments](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/fulfillment)
* [Inventory Items](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/inventoryItems?example=Retrieves+a+detailed+list+for+inventory+items+by+IDs)
   * Inventory Levels
* [Locations](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/locations?example=Retrieve+a+list+of+locations)
   * Location Metafields
* [Orders](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/orders?example=Retrieve+a+list+of+orders)
   * Order Agreements
   * Order Metafields
   * Order Refunds
   * Order Risks
   * Order Transactions
* [Product Variants](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/productvariants)
* [Products](https://shopify.dev/docs/api/admin-graphql/2025-01/queries/products)
   * Product Media
   * Product Metafields
* [Smart Collections](https://shopify.dev/docs/api/admin-graphql/2025-04/queries/collections)
   * Smart Collection Metafields

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* Store ID of your Shopify account. This is the prefix of your admin URL. For example, `https://{store_id}.myshopify.com/admin`

You can authenticate your account either via OAuth or with a Shopify [access token](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/generate-app-access-tokens-admin).

### Access Token Permissions

If authenticating with an access token, ensure the following permissions are granted:
* `read_assigned_fulfillment_orders`
* `read_checkouts`
* `read_customers`
* `read_fulfillments`
* `read_inventory`
* `read_locales`
* `read_locations`
* `read_marketplace_fulfillment_orders`
* `read_merchant_managed_fulfillment_orders`
* `read_orders`
* `read_payment_terms`
* `read_products`
* `read_publications`

### Bulk Query Operation Limitations

This connector submits and process the results of [bulk query operations](https://shopify.dev/docs/api/admin-graphql/2025-01/mutations/bulkoperationrunquery) to capture data. Shopify only allows a [single bulk query operation to run at a given time](https://shopify.dev/docs/api/usage/bulk-operations/queries#limitations). To ensure the connector can successfully submit bulk queries, ensure no other applications are submitting bulk query operations for your Shopify store.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Shopify source connector.

### Properties

#### Endpoint

The properties in the table below reflect manual authentication using the CLI. In the Flow web app,
you'll sign in directly and won't need the access token.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/store`** | Store ID | Your Shopify Store ID. Use the prefix of your admin URL e.g. `https://{store_id}.myshopify.com/admin`.  | string | Required |
| `/start_date` | Start date | UTC date and time in the format 2025-01-16T00:00:00Z. Any data before this date will not be replicated. | string | 30 days before the present date |
| **`/credentials/access_token`** | Access Token | Shopify access token. | string | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required |
| `/advanced/window_size` | Window size | Window size in days for incrementals streams. Typically left as the default unless more frequent checkpoints are desired. | integer | 30 |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-shopify-native:dev
        config:
            advanced:
                window_size: 30
            credentials:
                credentials_title: Private App Credentials
                access_token: <secret>
            start_date: "2025-01-16T12:00:00Z"
            store: <store ID>
    bindings:
      - resource:
          name: products
        target: ${PREFIX}/products
```

## Limitations with Custom App Access Tokens

If you authenticate using an access token from a custom app on a Shopify plan below the Grow tier, the following streams will not be discovered:

- `customers`
- `fulfillment_orders`
- `orders`

This is due to Shopify API [restrictions](https://help.shopify.com/en/manual/apps/app-types/custom-apps) for lower-tier plans.
