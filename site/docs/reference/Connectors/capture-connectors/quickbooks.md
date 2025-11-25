# QuickBooks

This connector captures data from QuickBooks into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-quickbooks:dev`](https://ghcr.io/estuary/source-quickbooks:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the QuickBooks API:

- [Accounts](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/account)
- [Bill Payments](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/billpayment)
- [Bills](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/bill)
- [Budgets](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/budget)
- [Classes](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/class)
- [Credit Memos](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/creditmemo)
- [Customers](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/customer)
- [Departments](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/department)
- [Deposits](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/deposit)
- [Employees](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/employee)
- [Estimates](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/estimate)
- [Invoices](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/invoice)
- [Items](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/item)
- [Journal Entries](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/journalentry)
- [Payment Methods](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/paymentmethod)
- [Payments](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/payment)
- [Purchase Orders](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/purchaseorder)
- [Purchases](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/purchase)
- [Refund Receipts](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/refundreceipt)
- [Sales Receipts](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/salesreceipt)
- [Tax Agencies](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/taxagency)
- [Tax Codes](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/taxcode)
- [Tax Rates](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/taxrate)
- [Terms](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/term)
- [Time Activities](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/timeactivity)
- [Transfers](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/transfer)
- [Vendor Credits](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/vendorcredit)
- [Vendors](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/vendor)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To set up the QuickBooks source connector, you'll need a [QuickBooks company ID](https://quickbooks.intuit.com/learn-support/en-global/help-article/customer-company-settings/find-quickbooks-online-company-id/L7lp8O9yU_ROW_en).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the QuickBooks source connector.

### Properties

#### Endpoint

| Property                                   | Title                            | Description                                                                                                                                 | Type   | Required/Default     |
| ------------------------------------------ | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------ | -------------------- |
| **`/realm_id`**                            | Company ID                       | ID for the Company to Request Data From                                                                                                     | string | Required             |
| **`/credentials/access_token`**            | Access Token                     | The access token received from the OAuth app.                                                                                               | string | Required             |
| **`/credentials/access_token_expires_at`** | Access Token Expiration Datetime | The access token's expiration date and time in the format 2025-04-24T00:00:00Z.                                                             | string | Required             |
| **`/credentials/client_id`**               | OAuth Client ID                  | The OAuth app's client ID.                                                                                                                  | string | Required             |
| **`/credentials/client_secret`**           | OAuth Client Secret              | The OAuth app's client secret.                                                                                                              | string | Required             |
| **`/credentials/credentials_title`**       | Authentication Method            | Name of the credentials set. Set to `OAuth Credentials`.                                                                                    | string | Required             |
| **`/credentials/refresh_token`**           | Refresh Token                    | The refresh token received from the OAuth app.                                                                                              | string | Required             |
| `/start_date`                              | Start Date                       | The date from which you'd like to replicate data, in the format YYYY-MM-DDT00:00:00Z. All data modified after this date will be replicated. | string | Default: 30 days ago |

#### Bindings

| Property        | Title            | Description                                                         | Type   | Required/Default   |
| --------------- | ---------------- | ------------------------------------------------------------------- | ------ | ------------------ |
| **`/name`**     | Resource Name    | Name of the QuickBooks resource from which collections are captured | string | Required           |
| **`/interval`** | Polling Interval | Frequency at which to poll for new data                             | string | Default: 5 minutes |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-quickbooks:dev
        config:
          realm_id: <your realm id>
          credentials:
            access_token: <secret>
            access_token_expires_at: "2025-04-24T12:00:00Z"
            credentials_title: "OAuth Credentials"
            client_id: <secret>
            client_secret: <secret>
            refresh_token: <secret>
          start_date: 2024-01-01T00:00:00Z
    bindings:
      - resource:
          name: Invoices
          interval: PT5M
        target: ${PREFIX}/invoices
      - resource:
          name: Customers
          interval: PT5M
        target: ${PREFIX}/customers
```
