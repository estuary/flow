
# Paypal Transaction

This connector captures data from Paypal Transaction into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-paypal-transaction:dev`](https://ghcr.io/estuary/source-paypal-transaction:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Paypal APIs:

* [Transactions](https://developer.paypal.com/docs/api/transaction-search/v1/#transactions)
* [Balances](https://developer.paypal.com/docs/api/transaction-search/v1/#balances)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* The [Paypal Transaction API](https://developer.paypal.com/docs/api/transaction-search/v1/) is used to get the history of transactions for a PayPal account.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Paypal Transaction source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/client_id` | Client ID | The Client ID of your Paypal developer application. | string | Required |
| `/client_secret` | Client Secret | The Client Secret of your Paypal developer application. | string | Required |
| `/is_sandbox` | Sandbox | Checkbox to indicate whether it is a sandbox environment or not | boolean | `false` |
| `/refresh_token` | Refresh token | The key to refresh the expired access token. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Paypal Transaction project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-paypal-transaction:dev
        config:
          client_id: <secret>
          client_secret: <secret>
          is_sandbox: false
          refresh_token: <secret>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: transactions
          syncMode: full_refresh
        target: ${PREFIX}/transactions
      {...}
```
