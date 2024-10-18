
# Chargebee

This connector captures data from Chargebee into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-chargebee:dev`](https://ghcr.io/estuary/source-chargebee:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Chargebee APIs:

* [Subscriptions](https://apidocs.chargebee.com/docs/api/subscriptions?prod_cat_ver=2#list_subscriptions)
* [Customers](https://apidocs.chargebee.com/docs/api/customers?prod_cat_ver=2#list_customers)
* [Invoices](https://apidocs.chargebee.com/docs/api/invoices?prod_cat_ver=2#list_invoices)
* [Orders](https://apidocs.chargebee.com/docs/api/orders?prod_cat_ver=2#list_orders)
* [Plans](https://apidocs.chargebee.com/docs/api/plans?prod_cat_ver=1&lang=curl#list_plans)
* [Addons](https://apidocs.chargebee.com/docs/api/addons?prod_cat_ver=1&lang=curl#list_addons)
* [Items](https://apidocs.chargebee.com/docs/api/items?prod_cat_ver=2#list_items)
* [Item Prices](https://apidocs.chargebee.com/docs/api/item_prices?prod_cat_ver=2#list_item_prices)
* [Attached Items](https://apidocs.chargebee.com/docs/api/attached_items?prod_cat_ver=2#list_attached_items)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Chargebee source connector, you'll need the [Chargebee API key](https://apidocs.chargebee.com/docs/api?prod_cat_ver=2#api_authentication) and the [Product Catalog version](https://apidocs.chargebee.com/docs/api?prod_cat_ver=2).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Chargebee source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/site_api_key` | API Key | Chargebee API Key. | string | Required |
| `/site` | Site | The site prefix for your Chargebee instance. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |
| `/product_catalog` | Product Catalog | Product Catalog version of your Chargebee site. Instructions on how to find your version you may find under 'API Version' section [in the Chargebee docs](https://apidocs.chargebee.com/docs/api/versioning?prod_cat_ver=2). | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Chargebee project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-chargebee:dev
        config:
          site_api_key: <secret>
          site: <your site>
          start_date: 2017-01-25T00:00:00Z
          product_catalog: <your product catalog>
    bindings:
      - resource:
          stream: items
          syncMode: full_refresh
        target: ${PREFIX}/items
      {...}
```
