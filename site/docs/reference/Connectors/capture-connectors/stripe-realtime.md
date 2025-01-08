# Stripe Real-time

This connector captures data from [Stripe's API](https://docs.stripe.com/api) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-stripe-native:dev`](https://ghcr.io/estuary/source-stripe-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.


## Supported data resources

The following data resources are supported through the Stripe API:

* [Accounts](https://docs.stripe.com/api/accounts/list)
* [Application fees](https://docs.stripe.com/api/application_fees/list)
* [Application fees refunds](https://docs.stripe.com/api/fee_refunds/list)
* [Balance transactions](https://docs.stripe.com/api/balance_transactions/list)
* [Bank accounts](https://docs.stripe.com/api/customer_bank_accounts/list)
* [Cards](https://docs.stripe.com/api/cards/list)
* [Charges](https://docs.stripe.com/api/charges/list)
* [Checkout sessions](https://docs.stripe.com/api/checkout/sessions/list)
* [Checkout sessions line items](https://docs.stripe.com/api/checkout/sessions/line_items)
* [Coupons](https://docs.stripe.com/api/coupons/list)
* [Credit notes](https://docs.stripe.com/api/credit_notes/list)
* [Credit notes line items](https://docs.stripe.com/api/credit_notes/lines)
* [Customer balance transactions](https://docs.stripe.com/api/customer_balance_transactions/list)
* [Customers](https://docs.stripe.com/api/customers/list)
* [Disputes](https://docs.stripe.com/api/disputes/list)
* [Early fraud warning](https://docs.stripe.com/api/radar/early_fraud_warnings/list)
* [External account cards](https://docs.stripe.com/api/external_account_cards/list)
* [External bank account](https://docs.stripe.com/api/external_account_bank_accounts/list)
* [Files](https://docs.stripe.com/api/files/list)
* [File links](https://docs.stripe.com/api/file_links/list)
* [Invoice items](https://docs.stripe.com/api/invoiceitems/list)
* [Invoice line items](https://docs.stripe.com/api/invoice-line-item/retrieve)
* [Invoices](https://docs.stripe.com/api/invoices/list)
* [Payment intents](https://docs.stripe.com/api/payment_intents/list)
* [Payment methods](https://docs.stripe.com/api/payment_methods/list)
* [Payouts](https://docs.stripe.com/api/payouts/list)
* [Persons](https://docs.stripe.com/api/persons/list)
* [Plans](https://docs.stripe.com/api/plans/list)
* [Products](https://docs.stripe.com/api/products/list)
* [Promotion codes](https://docs.stripe.com/api/promotion_codes/list)
* [Refunds](https://docs.stripe.com/api/refunds/list)
* [Reviews](https://docs.stripe.com/api/radar/reviews/list)
* [Setup attempts](https://docs.stripe.com/api/setup_attempts/list)
* [Setup intents](https://docs.stripe.com/api/setup_intents/list)
* [Subscription items](https://docs.stripe.com/api/subscription_items/list)
* [Subscriptions](https://docs.stripe.com/api/subscriptions/list)
* [Subscription schedule](https://docs.stripe.com/api/subscription_schedules/list)
* [Top ups](https://docs.stripe.com/api/topups/list)
* [Transfer reversals](https://docs.stripe.com/api/transfer_reversals/list)
* [Transfers](https://docs.stripe.com/api/transfers/list)
* [Usage records](https://docs.stripe.com/api/usage-record-summary/list)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* An [API Key](https://docs.stripe.com/keys) for your Stripe account. This usually starts with `sk_live_` or `sk_test_` depending on your environment. Manage your Stripe keys in their [developer dashboard](https://dashboard.stripe.com/test/apikeys).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Stripe source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials |  | object | Required |
| `/credentials/credentials_title` | Credentials Title | The type of authentication. Currently only accepts `Private App Credentials`. | string | `Private App Credentials` |
| `/credentials/access_token` | Access Token | Stripe API key. Usually starts with `sk_live_`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Only data generated after this date will be replicated. | string | 30 days before the present date |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource from Stripe from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-stripe-native:dev
        config:
            credentials:
                credentials_title: Private App Credentials
                access_token: <secret>
            start_date: 2025-01-01T00:00:00Z
    bindings:
      - resource:
          stream: charges
          syncMode: incremental
        target: ${PREFIX}/charges
      - resource:
          stream: customer_balance_transactions
          syncMode: full_refresh
        target: ${PREFIX}/customerbalancetransactions
    {...}
```
