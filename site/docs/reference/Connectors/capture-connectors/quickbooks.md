---
description: Capture QuickBooks accounting data with Estuary, including invoices, customers, bills, payments, and vendors, with a simple OAuth connection.
---

# QuickBooks

This connector captures data from QuickBooks into Estuary collections.

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

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

This connector authenticates with your own Intuit app. You'll need:

- An [Intuit developer account](https://developer.intuit.com) and an app with access to the QuickBooks Online Accounting API
- Your app's **client ID** and **client secret**
- A **refresh token** obtained by authorizing your app against your QuickBooks company
- Your [QuickBooks company ID](https://quickbooks.intuit.com/learn-support/en-global/help-article/customer-company-settings/find-quickbooks-online-company-id/L7lp8O9yU_ROW_en) (also called the realm ID)

### Create an Intuit app

1. Sign in (or sign up) at the [Intuit Developer Portal](https://developer.intuit.com) and create a new app, granting it the **QuickBooks Online Accounting API** scope (`com.intuit.quickbooks.accounting`).
2. Your app starts with **Development** keys, which only work against [sandbox companies](https://developer.intuit.com/app/developer/qbo/docs/develop/sandboxes). To capture data from a real QuickBooks company, complete the questionnaire under your app's **Production Settings** to obtain **Production** keys.
3. In your app's **Keys & credentials** page (under Production Settings for a real company, or Development Settings for a sandbox), note the **Client ID** and **Client Secret**.

### Obtain a refresh token with the OAuth 2.0 Playground

The easiest way to authorize your app and generate a refresh token is Intuit's [OAuth 2.0 Playground](https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0-playground):

1. On your app's **Keys & credentials** page, add the playground's redirect URI (`https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl`) to the app's Redirect URIs. Do this for the environment (Production or Development) whose keys you're using.
2. Open the OAuth 2.0 Playground, select your app and environment, and check the `com.intuit.quickbooks.accounting` scope.
3. Click **Get authorization code** and authorize the QuickBooks company you want to capture from. The playground displays the company's **realm ID**.
4. Click **Get tokens**. The response contains your **refresh token**.

Refresh tokens are valid for up to 100 days. That's all the connector needs: it exchanges the refresh token for access tokens on its own, keeping the tokens in your endpoint configuration up to date. Create the capture reasonably soon after generating the refresh token — Intuit issues a replacement refresh token roughly every 24 hours, and only the latest one remains valid.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the QuickBooks source connector.

### Properties

#### Endpoint

| Property                             | Title                   | Description                                                                                                                                 | Type    | Required/Default     |
| ------------------------------------ | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------- |
| **`/realm_id`**                      | Company ID              | ID for the Company to Request Data From                                                                                                     | string  | Required             |
| **`/credentials/credentials_title`** | Authentication Method   | Name of the credentials set. Set to `OAuth Credentials`.                                                                                    | string  | Required             |
| **`/credentials/client_id`**         | Client ID               | Your Intuit app's client ID.                                                                                                                | string  | Required             |
| **`/credentials/client_secret`**     | Client Secret           | Your Intuit app's client secret.                                                                                                            | string  | Required             |
| **`/credentials/refresh_token`**     | Refresh Token           | The refresh token received when authorizing your app.                                                                                       | string  | Required             |
| `/start_date`                        | Start Date              | The date from which you'd like to replicate data, in the format YYYY-MM-DDT00:00:00Z. All data modified after this date will be replicated. | string  | Default: 30 days ago |
| `/is_sandbox`                        | Use Sandbox Environment | Enable to capture from a QuickBooks sandbox company instead of production.                                                                  | boolean | Default: false       |

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
        image: ghcr.io/estuary/source-quickbooks:v1
        config:
          realm_id: <your realm id>
          credentials:
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
