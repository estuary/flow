# Recharge

This connector captures data from Recharge into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-recharge:dev`](https://ghcr.io/estuary/source-recharge:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Recharge APIs:

* [Addresses](https://developer.rechargepayments.com/v1-shopify?python#list-addresses)
* [Charges](https://developer.rechargepayments.com/v1-shopify?python#list-charges)
* [Collections](https://developer.rechargepayments.com/v1-shopify)
* [Customers](https://developer.rechargepayments.com/v1-shopify?python#list-customers)
* [Discounts](https://developer.rechargepayments.com/v1-shopify?python#list-discounts)
* [Metafields](https://developer.rechargepayments.com/v1-shopify?python#list-metafields)
* [Onetimes](https://developer.rechargepayments.com/v1-shopify?python#list-onetimes)
* [Orders](https://developer.rechargepayments.com/v1-shopify?python#list-orders)
* [Products](https://developer.rechargepayments.com/v1-shopify?python#list-products)
* [Shop](https://developer.rechargepayments.com/v1-shopify?python#shop)
* [Subscriptions](https://developer.rechargepayments.com/v1-shopify?python#list-subscriptions)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* Recharge Access Token for authentication.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Recharge source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/access_token` | Access Token | The value of the Access Token generated. | string | Required |
| `/start_date` | Start Date | The date from which you'd like to replicate data for Recharge API, in the format YYYY-MM-DDT00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Recharge project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-recharge:dev
        config:
          access_token: <secret>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: addresses
          syncMode: full_refresh
        target: ${PREFIX}/addresses
      {...}
```