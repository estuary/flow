# Braintree

This connector captures data from Braintree into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-braintree-native:dev`](https://ghcr.io/estuary/source-braintree-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The connector automatically discovers bindings for the Braintree resources listed below. By default, each resource is mapped to a Flow collection through a separate binding.

### Full Refresh Streams

* [Add Ons](https://developer.paypal.com/braintree/docs/reference/response/add-on/python)
* [Discounts](https://developer.paypal.com/braintree/docs/reference/response/discount/python)
* [Merchant Accounts](https://developer.paypal.com/braintree/docs/reference/response/merchant-account/python)
* [Plans](https://developer.paypal.com/braintree/docs/reference/response/plan/python)

### Incremental Streams

* [Credit Card Verifications](https://developer.paypal.com/braintree/docs/reference/response/credit-card-verification/python)
* [Customers](https://developer.paypal.com/braintree/docs/reference/response/customer/python)
* [Disputes](https://developer.paypal.com/braintree/docs/reference/request/dispute/search/python)
* [Subscriptions](https://developer.paypal.com/braintree/docs/reference/response/subscription/python)
* [Transactions](https://developer.paypal.com/braintree/docs/reference/response/transaction/python)

:::tip
Incremental streams only capture **creates**, not updates, of resources due to Braintree API limitations. To capture updates to these resources, regular backfills are required. Please reach out via [email](mailto:support@estuary.dev) or [Slack](https://go.estuary.dev/slack) to set up and schedule regular backfills.
:::

## Prerequisites

To set up the Braintree source connector, you'll need the following from your Braintree account:
1. [Merchant ID](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#merchant-id)
2. [Public Key](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#public-key)
3. [Private Key](https://developer.paypal.com/braintree/articles/control-panel/important-gateway-credentials#private-key)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Braintree source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/merchant_id`** | Merchant ID | The unique identifier for your Braintree gateway account. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format "YYYY-MM-DDTHH:MM:SSZ". Any data before this date will not be replicated. | string | 30 days prior to the current date |
| **`/credentials/public_key`** | Public Key | Braintree Public Key. | string | Required |
| **`/credentials/private_key`** | Private Key | Braintree Private Key. | string | Required |
| `/advanced/is_sandbox` | Sandbox Environment | Set to `true` if the credentials are for a [sandbox](https://developer.paypal.com/braintree/articles/get-started/try-it-out#the-braintree-sandbox) Braintree environment. | boolean | `false` |
| `/advanced/window_size` | Window Size | The window size in hours to use when fetching data from Braintree. Typically, this is left as the default value unless the connector raises an error stating that the window size needs to be reduced.| integer | 24 |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | PT5M |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-braintree-native:dev
        config:
          merchant_id: my_merchant_id
          start_date: "2024-12-04T00:00:00Z"
          credentials:
            public_key: my_public_key
            private_key: my_private_key
          advanced:
            is_sandbox: false
            window_size: 15
    bindings:
      - resource:
          name: add_ons
          interval: PT5M
        target: ${PREFIX}/add_ons
      - resource:
          name: credit_card_verifications
          interval: PT5M
        target: ${PREFIX}/credit_card_verifications
      - resource:
          name: customers
          interval: PT5M
        target: ${PREFIX}/customers
      - resource:
          name: discounts
          interval: PT5M
        target: ${PREFIX}/discounts
      - resource:
          name: disputes
          interval: PT5M
        target: ${PREFIX}/disputes
      - resource:
          name: merchant_accounts
          interval: PT5M
        target: ${PREFIX}/merchant_accounts
      - resource:
          name: merchant_accounts
          interval: PT5M
        target: ${PREFIX}/merchant_accounts
      - resource:
          name: plans
          interval: PT5M
        target: ${PREFIX}/plans
      - resource:
          name: subscriptions
          interval: PT5M
        target: ${PREFIX}/subscriptions
      - resource:
          name: transactions
          interval: PT5M
        target: ${PREFIX}/transactions
      {...}
```
