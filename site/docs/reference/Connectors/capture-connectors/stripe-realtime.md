# Stripe Real-time

This connector captures data from [Stripe's API](https://docs.stripe.com/api) into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-stripe-native:dev`](https://ghcr.io/estuary/source-stripe-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.


## Data synchronization

The Stripe connector uses a combination of real-time event streaming and periodic backfills to ensure data consistency. Most streams capture changes in real-time via Stripe's Events API, but some streams require scheduled backfills due to limitations in Stripe's event generation.

### Streams requiring scheduled backfills

The following streams are configured with daily scheduled backfills (at midnight UTC) to ensure eventual consistency:

| Stream | Reason for Scheduled Backfill |
|--------|-------------------------------|
| **Accounts** | Stripe's event generation for accounts is inconsistent. While webhooks may receive `account.updated` events, the Events API endpoint may not reliably surface them. This stream uses the list endpoint directly for incremental capture. |
| **Persons** | Events for person records may not consistently appear in Stripe's Events API. Persons are child records of connected accounts. |
| **ExternalAccountCards** | Events for external account cards may not consistently appear in Stripe's Events API. These are child records accessed via connected account endpoints. |
| **ExternalBankAccount** | Events for external bank accounts may not consistently appear in Stripe's Events API. These are child records accessed via connected account endpoints. |

These scheduled backfills provide guaranteed eventual consistency by periodically re-querying the list endpoints, ensuring that no data is missed even if Stripe fails to generate corresponding events.

### Stripe API limitations

The connector handles several known limitations of the Stripe API:

- **Inconsistent event generation**: Stripe's Events API may not surface events for certain resource types, even when corresponding webhooks are delivered. This particularly affects account-related resources.
- **Connected account child streams**: Resources like Persons, ExternalAccountCards, and ExternalBankAccount must be queried through parent account endpoints (e.g., `/v1/accounts/{id}/persons`), requiring per-account queries when capturing connected accounts.
- **Events API retention**: Stripe retains events for 30 days. The connector uses a configurable `start_date` to control how far back to capture historical data during initial backfill.

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
* [Events](https://docs.stripe.com/api/events/list)
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

By default, each resource is mapped to an Estuary collection through a separate binding.

### Connected Accounts

This connector can capture data from Stripe Connected Accounts. To enable this feature, set the `capture_connected_accounts` property to `true` in your configuration. When enabled, each document will include an `account_id` field that identifies which account the data belongs to.

#### How connected account capture works

When `capture_connected_accounts` is enabled, the connector handles different streams in different ways:

- **Accounts stream**: Always queries from the platform account using the `/v1/accounts` endpoint to list all connected accounts. This stream does not create per-account subtasks.
- **Most other streams**: Create per-account subtasks that query each connected account's data using the `Stripe-Account` header to access account-specific resources.
- **Child streams** (Persons, ExternalAccountCards, ExternalBankAccount): Query each connected account's child resources via parent endpoints (e.g., `/v1/accounts/{id}/persons`).

This architecture ensures efficient data capture while respecting Stripe's API access patterns for connected accounts.

#### Data consistency considerations

For captures with many connected accounts, the connector uses a priority-based rotation system to fairly process accounts based on how recently they were synced. Combined with scheduled backfills for streams with unreliable event generation, this ensures eventual consistency across all connected accounts.

## Prerequisites

* An [API Key](https://docs.stripe.com/keys) for your Stripe account. This usually starts with `sk_live_` or `sk_test_` depending on your environment. Manage your Stripe keys in their [developer dashboard](https://dashboard.stripe.com/test/apikeys).

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Stripe source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials |  | object | Required |
| `/credentials/credentials_title` | Credentials Title | The type of authentication. Currently only accepts `Private App Credentials`. | string | `Private App Credentials` |
| `/credentials/access_token` | Access Token | Stripe API key. Usually starts with `sk_live_`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Only data generated after this date will be replicated. | string | 30 days before the present date |
| `/capture_connected_accounts` | Capture Connected Accounts | Whether to capture data from connected accounts. | boolean | `false` |`

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
            capture_connected_accounts: true
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
