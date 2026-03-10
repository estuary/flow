
# Shopify (GraphQL)

This connector captures data from [Shopify's GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql) into Estuary collections.

## Supported data resources

The following data resources are supported through the Shopify API:

* [Abandoned Checkouts](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/abandonedCheckouts?example=Retrieves+a+list+of+abandoned+checkouts)
* [Custom Collections](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/collections?example=Retrieves+a+list+of+custom+collections)
   * Custom Collection Metafields
* [Customers](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/customers)
   * Customer Metafields
* [Fulfillment Orders](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/fulfillmentorders)
* [Fulfillments](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/fulfillment)
* [Inventory Items](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/inventoryItems?example=Retrieves+a+detailed+list+for+inventory+items+by+IDs)
   * Inventory Levels
* [Locations](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/locations?example=Retrieve+a+list+of+locations)
   * Location Metafields
* [Orders](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/orders?example=Retrieve+a+list+of+orders)
   * Order Agreements
   * Order Metafields
   * Order Refunds
   * Order Risks
   * Order Transactions
* [Product Variants](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/productvariants)
* [Products](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/products)
   * Product Media
   * Product Metafields
* [Smart Collections](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/collections)
   * Smart Collection Metafields
* [Subscription Contracts](https://shopify.dev/docs/api/admin-graphql/2026-01/queries/subscriptioncontracts)


By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

* One or more Shopify store IDs. Each store ID is the prefix of your admin URL. For example, `https://{store_id}.myshopify.com/admin`

You can authenticate each store via OAuth, with a Shopify [access token](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/generate-app-access-tokens-admin), or with [client credentials](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/client-credentials-grant).

### Authentication Methods

#### Private App Credentials (Access Token)

For existing custom apps with static access tokens (`shpat_*` tokens). The access token is configured directly in the connector and does not expire.

:::note
Shopify has deprecated the creation of legacy custom apps. Existing access tokens continue to work, but new apps must use Client Credentials. See [Shopify's custom app documentation](https://help.shopify.com/en/manual/apps/app-types/custom-apps) for details.
:::

#### Client Credentials

For custom apps created through the [Shopify Dev Dashboard](https://help.shopify.com/en/manual/apps/app-types/custom-apps). Uses `client_id` and `client_secret` to obtain short-lived access tokens (~24 hours) that auto-refresh at runtime. This is the recommended authentication method for new integrations.

### Required Permissions

When authenticating with an access token or client credentials, ensure the following permissions are granted:
* `read_assigned_fulfillment_orders`
* `read_checkouts`
* `read_customers`
* `read_fulfillments`
* `read_inventory`
* `read_locales`
* `read_locations`
* `read_marketing_events`
* `read_marketplace_fulfillment_orders`
* `read_merchant_managed_fulfillment_orders`
* `read_orders`
* `read_payment_terms`
* `read_products`
* `read_publications`
* `read_own_subscription_contracts`

### Bulk Query Operation Limitations

This connector submits and processes the results of [bulk query operations](https://shopify.dev/docs/api/admin-graphql/2026-01/mutations/bulkoperationrunquery) to capture data. As of API version 2026-01, Shopify supports up to [5 concurrent bulk query operations](https://shopify.dev/docs/api/usage/bulk-operations/queries#limitations). The connector takes advantage of this to run multiple bulk queries in parallel for improved performance.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Shopify source connector.

### Properties

#### Endpoint

The properties in the table below reflect manual authentication using the CLI. In the Estuary web app,
you'll sign in directly and won't need the access token.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stores`** | Shopify Stores | One or more Shopify stores to capture. Each store requires its own credentials. | array | Required |
| **`/stores/[]/store`** | Store Name | Store name (the prefix of your admin URL, e.g., `mystore` for `mystore.myshopify.com`) | string | Required |
| **`/stores/[]/credentials`** | Authentication | Store credentials. See [Authentication Methods](#authentication-methods). | object | Required |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data before this date will not be replicated. | string | 30 days before the present date |
| `/advanced/window_size` | Window Size | Window size for incremental streams in ISO 8601 format (e.g., P30D means 30 days, PT6H means 6 hours). | string | P30D |
| `/advanced/should_use_composite_key` | Use Composite Key | Include store identifier (`/_meta/store`) in collection keys. Enabled by default for new captures. Set to `true` and backfill all bindings before adding stores to a legacy capture. | boolean | `true` (new) / `false` (legacy) |

**Credential properties for Private App Credentials:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stores/[]/credentials/credentials_title`** | Credentials Title | Must be `Private App Credentials` | string | Required |
| **`/stores/[]/credentials/access_token`** | Access Token | Shopify access token | string | Required |

**Credential properties for Client Credentials:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stores/[]/credentials/credentials_title`** | Credentials Title | Must be `Client Credentials` | string | Required |
| **`/stores/[]/credentials/client_id`** | Client ID | OAuth2 client ID | string | Required |
| **`/stores/[]/credentials/client_secret`** | Client Secret | OAuth2 client secret | string | Required |

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
        image: ghcr.io/estuary/source-shopify-native:v2
        config:
          stores:
            - store: primary-store
              credentials:
                credentials_title: Private App Credentials
                access_token: <secret>
            - store: secondary-store
              credentials:
                credentials_title: Client Credentials
                client_id: <client_id>
                client_secret: <secret>
          start_date: "2025-01-16T12:00:00Z"
    bindings:
      - resource:
          name: products
        target: ${PREFIX}/products
      - resource:
          name: orders
        target: ${PREFIX}/orders
```

## Multi-Store Captures

This connector supports capturing data from multiple Shopify stores in a single capture task. Each store authenticates independently and can use different credential types.

### How it works

- Configure multiple stores in the `stores` array, each with its own store name and credentials
- All documents include a `/_meta/store` field identifying which store they originated from
- Resources from all stores are captured into the same collections
- Collection keys include the store identifier (`/_meta/store`) to ensure uniqueness across stores

### Adding stores to an existing capture

If you have an existing single-store capture created before multi-store support was added, the `should_use_composite_key` advanced option will be set to `false`. To add additional stores:

1. Set `/advanced/should_use_composite_key` to `true`
2. Perform a backfill and dataflow reset for all bindings

This is required because the collection key structure must include the store identifier (`/_meta/store`) to ensure uniqueness between documents.

## Limitations with Custom App Access Tokens

If you authenticate using an access token from a custom app on a Shopify plan below the Grow tier, the following streams will not be discovered:

- `customers`
- `fulfillment_orders`
- `orders`

This is due to Shopify API [restrictions](https://help.shopify.com/en/manual/apps/app-types/custom-apps) for lower-tier plans.
