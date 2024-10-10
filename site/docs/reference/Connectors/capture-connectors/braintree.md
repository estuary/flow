
# Braintree

This connector captures data from Braintree into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-braintree:dev`](https://ghcr.io/estuary/source-braintree:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Braintree APIs:

* [Customers](https://developer.paypal.com/braintree/docs/reference/request/customer/search)
* [Discounts](https://developer.paypal.com/braintree/docs/reference/response/discount)
* [Disputes](https://developer.paypal.com/braintree/docs/reference/request/dispute/search)
* [Transactions](https://developers.braintreepayments.com/reference/response/transaction/python)
* [Merchant Accounts](https://developer.paypal.com/braintree/docs/reference/response/merchant-account)
* [Plans](https://developer.paypal.com/braintree/docs/reference/response/plan)
* [Subscriptions](https://developer.paypal.com/braintree/docs/reference/response/subscription)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To set up the Braintree source connector, you'll need Braintree's:

1. [Public Key](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#public-key)
2. [Environment](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#environment)
3. [Merchant ID](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#merchant-id)
4. [Private Key](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#private-key)

We recommend creating a restricted, read-only key specifically for Estuary access. This will allow you to control which resources Estuary should be able to access.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Braintree source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/environment` | Environment | Environment specifies where the data will come from. | string | Required |
| `/merchant_id` | Merchant ID | The unique identifier for your entire gateway account. | string | Required |
| `/private_key` | Private Key | Braintree Private Key. | string | Required |
| `/public_key` | Public Key | Braintree Public Key. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Default |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Braintree project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-braintree:dev
        config:
          environment: Development
          merchant_id: <id>
          private_key: <key>
          public_key: <key>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: customers
          syncMode: full_refresh
        target: ${PREFIX}/customers
      {...}
```
