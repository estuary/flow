# Stripe (Deprecated)

This connector captures data from Stripe into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-stripe:dev`](https://ghcr.io/estuary/source-stripe:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

:::warning
This connector is deprecated. See the [Stripe Real-time](./stripe-realtime.md) connector for the latest Stripe integration.
:::

## Supported data resources

The following data resources are supported through the Stripe API:

* [Balance transactions](https://stripe.com/docs/api/balance_transactions/list)
* [Bank accounts](https://stripe.com/docs/api/customer_bank_accounts/list)
* [Charges](https://stripe.com/docs/api/charges/list)
* [Checkout sessions](https://stripe.com/docs/api/checkout/sessions/list)
* [Checkout sessions line items](https://stripe.com/docs/api/checkout/sessions/line_items)
* [Coupons](https://stripe.com/docs/api/coupons/list)
* [Customer balance transactions](https://stripe.com/docs/api/customer_balance_transactions/list)
* [Customers](https://stripe.com/docs/api/customers/list)
* [Disputes](https://stripe.com/docs/api/disputes/list)
* [Events](https://stripe.com/docs/api/events/list)
* [Invoice items](https://stripe.com/docs/api/invoiceitems/list)
* [Invoice line items](https://stripe.com/docs/api/invoices/invoice_lines)
* [Invoices](https://stripe.com/docs/api/invoices/list)
* [Payment intents](https://stripe.com/docs/api/payment_intents/list)
* [Payouts](https://stripe.com/docs/api/payouts/list)
* [Plans](https://stripe.com/docs/api/plans/list)
* [Products](https://stripe.com/docs/api/products/list)
* [Promotion codes](https://stripe.com/docs/api/promotion_codes/list)
* [Refunds](https://stripe.com/docs/api/refunds/list)
* [Subscription items](https://stripe.com/docs/api/subscription_items/list)
* [Subscriptions](https://stripe.com/docs/api/subscriptions/list)
* [Transfers](https://stripe.com/docs/api/transfers/list)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* [Account ID](https://stripe.com/docs/dashboard/basics#find-account-id) of your Stripe account.
* [Secret key](https://stripe.com/docs/keys#obtain-api-keys) for the Stripe API.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Stripe source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/account_id`** | Account ID | Your Stripe account ID (starts with &#x27;acct&#x5F;&#x27;, find yours here https:&#x2F;&#x2F;dashboard.stripe.com&#x2F;settings&#x2F;account | string | Required |
| **`/client_secret`** | Secret Key | Stripe API key (usually starts with &#x27;sk&#x5F;live&#x5F;&#x27;; find yours here https:&#x2F;&#x2F;dashboard.stripe.com&#x2F;apikeys | string | Required |
| `/lookback_window_days` | Lookback Window in days (Optional) | When set, the connector will always re-export data from the past N days, where N is the value set here. This is useful if your data is frequently updated after creation. | integer | `0` |
| **`/start_date`** | Replication start date | UTC date and time in the format 2017-01-25T00:00:00Z. Only data generated after this date will be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource from Stripe from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |


### Choosing your start date and lookback window

The connector will continually capture data beginning on the **Replication start date** you choose.

However, some data from the Stripe API is mutable; for example, [a draft invoice can be completed](https://stripe.com/docs/billing/migration/invoice-states) at a later date than it was created.
To account for this, it's useful to set the **Lookback Window**. When this is set, at a given point in time, the connector will not only look for new data;
it will also capture changes made to data within the window.

For example, if you start the connector with the start date of `2022-06-06T00:00:00Z` (June 6) and the lookback window of `3`, the connector will begin to capture data starting from June 3.
As time goes on while the capture remains active, the lookback window rolls forward along with the current timestamp.
On June 10, the connector will continue to monitor data starting from June 7 and capture any changes to that data, and so on.

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-stripe:dev
        config:
            account_id: 00000000
            client_secret: <secret>
            start_date: 2022-06-18T00:00:00Z
    bindings:
      - resource:
          stream: balance_transactions
          syncMode: incremental
        target: ${PREFIX}/balancetransactions
      - resource:
          stream: bank_accounts
          syncMode: full_refresh
        target: ${PREFIX}/bankaccounts
      - resource:
          stream: charges
          syncMode: incremental
        target: ${PREFIX}/charges
      - resource:
          stream: checkout_sessions
          syncMode: incremental
        target: ${PREFIX}/checkoutsessions
      - resource:
          stream: checkout_sessions_line_items
          syncMode: incremental
        target: ${PREFIX}/checkoutsessionslineitems
      - resource:
          stream: coupons
          syncMode: incremental
        target: ${PREFIX}/coupons
      - resource:
          stream: customer_balance_transactions
          syncMode: full_refresh
        target: ${PREFIX}/customerbalancetransactions
      - resource:
          stream: customers
          syncMode: incremental
        target: ${PREFIX}/customers
      - resource:
          stream: disputes
          syncMode: incremental
        target: ${PREFIX}/disputes
      - resource:
          stream: events
          syncMode: incremental
        target: ${PREFIX}/events
      - resource:
          stream: invoice_items
          syncMode: incremental
        target: ${PREFIX}/invoice_items
      - resource:
          stream: invoice_line_items
          syncMode: full_refresh
        target: ${PREFIX}/invoicelineitems
      - resource:
          stream: invoices
          syncMode: incremental
        target: ${PREFIX}/invoices
      - resource:
          stream: payment_intents
          syncMode: incremental
        target: ${PREFIX}/paymentintents
      - resource:
          stream: payouts
          syncMode: incremental
        target: ${PREFIX}/payouts
      - resource:
          stream: plans
          syncMode: incremental
        target: ${PREFIX}/plans
      - resource:
          stream: products
          syncMode: incremental
        target: ${PREFIX}/products
      - resource:
          stream: promotion_codes
          syncMode: incremental
        target: ${PREFIX}/promotioncodes
      - resource:
          stream: refunds
          syncMode: incremental
        target: ${PREFIX}/refunds
      - resource:
          stream: subscription_items
          syncMode: full_refresh
        target: ${PREFIX}/subscriptionitems
      - resource:
          stream: subscriptions
          syncMode: incremental
        target: ${PREFIX}/subscriptions
      - resource:
          stream: transfers
          syncMode: incremental
        target: ${PREFIX}/transfers
```
