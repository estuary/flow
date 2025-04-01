# Chargebee Native

This connector captures data from Chargebee into Flow collections in real-time. It is a native implementation that provides enhanced performance and reliability compared to the third-party connector.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-chargebee-native:dev`](https://ghcr.io/estuary/source-chargebee-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The connector supports many Chargebee resources, which vary based on your Product Catalog version. Below is a comprehensive list of available resources and their capabilities:

| Stream                                                                                                                                      | Product Catalog 1.0 | Product Catalog 2.0 | Incremental | Full Refresh | Notes                                |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------- | ----------- | ------------ | ------------------------------------ |
| [Addons](https://apidocs.chargebee.com/docs/api/addons?prod_cat_ver=1#list_addons)                                                          | ✓                   | -                   | ✓           | -            | -                                    |
| [Attached Items](https://apidocs.chargebee.com/docs/api/attached_items?prod_cat_ver=2#list_attached_items)                                  | -                   | ✓                   | ✓           | -            | Associated with Items                |
| [Comments](https://apidocs.chargebee.com/docs/api/comments?prod_cat_ver=2#list_comments)                                                    | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Contacts](https://apidocs.chargebee.com/docs/api/customers?prod_cat_ver=2#list_of_contacts_for_a_customer)                                 | ✓                   | ✓                   | -           | ✓            | Associated with Customers            |
| [Coupons](https://apidocs.chargebee.com/docs/api/coupons?prod_cat_ver=2#list_coupons)                                                       | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Credit Notes](https://apidocs.chargebee.com/docs/api/credit_notes?prod_cat_ver=2#list_credit_notes)                                        | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Customers](https://apidocs.chargebee.com/docs/api/customers?prod_cat_ver=2#list_customers)                                                 | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Differential Prices](https://apidocs.chargebee.com/docs/api/differential_prices?prod_cat_ver=2#list_differential_prices)                   | -                   | ✓                   | -           | ✓            | -                                    |
| [Events](https://apidocs.chargebee.com/docs/api/events?prod_cat_ver=2#list_events)                                                          | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Gifts](https://apidocs.chargebee.com/docs/api/gifts?prod_cat_ver=2#list_gifts)                                                             | ✓                   | ✓                   | -           | ✓            | -                                    |
| [Hosted Pages](https://apidocs.chargebee.com/docs/api/hosted_pages?prod_cat_ver=2#list_hosted_pages)                                        | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Invoices](https://apidocs.chargebee.com/docs/api/invoices?prod_cat_ver=2#list_invoices)                                                    | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Item Families](https://apidocs.chargebee.com/docs/api/item_families?prod_cat_ver=2#list_item_families)                                     | -                   | ✓                   | ✓           | -            | -                                    |
| [Item Prices](https://apidocs.chargebee.com/docs/api/item_prices?prod_cat_ver=2#list_item_prices)                                           | -                   | ✓                   | ✓           | -            | -                                    |
| [Items](https://apidocs.chargebee.com/docs/api/items?prod_cat_ver=2#list_items)                                                             | -                   | ✓                   | ✓           | -            | -                                    |
| [Orders](https://apidocs.chargebee.com/docs/api/orders?prod_cat_ver=2#list_orders)                                                          | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Payment Sources](https://apidocs.chargebee.com/docs/api/payment_sources?prod_cat_ver=2#list_payment_sources)                               | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Plans](https://apidocs.chargebee.com/docs/api/plans?prod_cat_ver=1#list_plans)                                                             | ✓                   | -                   | ✓           | -            | -                                    |
| [Promotional Credits](https://apidocs.chargebee.com/docs/api/promotional_credits?prod_cat_ver=2#list_promotional_credits)                   | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Quote Line Groups](https://apidocs.chargebee.com/docs/api/quote_line_groups?prod_cat_ver=2#list_quote_line_groups)                         | ✓                   | ✓                   | -           | ✓            | Requires Performance/Enterprise plan |
| [Quotes](https://apidocs.chargebee.com/docs/api/quotes?prod_cat_ver=2#list_quotes)                                                          | ✓                   | ✓                   | ✓           | -            | Requires Performance/Enterprise plan |
| [Site Migration Details](https://apidocs.chargebee.com/docs/api/site_migration_details?prod_cat_ver=2#export_site_migration_detail)         | ✓                   | ✓                   | -           | ✓            | -                                    |
| [Subscriptions](https://apidocs.chargebee.com/docs/api/subscriptions?prod_cat_ver=2#list_subscriptions)                                     | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Subscriptions with Scheduled Changes](https://apidocs.chargebee.com/docs/api/subscriptions?prod_cat_ver=2#retrieve_with_scheduled_changes) | ✓                   | ✓                   | -           | ✓            | Associated with Subscriptions        |
| [Transactions](https://apidocs.chargebee.com/docs/api/transactions?prod_cat_ver=2#list_transactions)                                        | ✓                   | ✓                   | ✓           | -            | -                                    |
| [Unbilled Charges](https://apidocs.chargebee.com/docs/api/unbilled_charges?prod_cat_ver=2#list_unbilled_charges)                            | ✓                   | ✓                   | -           | ✓            | -                                    |
| [Virtual Bank Accounts](https://apidocs.chargebee.com/docs/api/virtual_bank_accounts?prod_cat_ver=2#list_virtual_bank_accounts)             | ✓                   | ✓                   | ✓           | -            | -                                    |

## Prerequisites

To set up the Chargebee Native source connector, you'll need:
* A Chargebee API key
* Your Chargebee site name
* Your Product Catalog version (1.0 or 2.0)

## Configuration

You can configure the connector either in the Flow web app or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Chargebee Native source connector.

### Properties

#### Endpoint

| Property                         | Title             | Description                                                                                             | Type   | Required/Default       |
| -------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------- | ------ | ---------------------- |
| `/credentials/api_key`           | API Key           | Chargebee API Key for authentication.                                                                   | string | Required               |
| `/credentials/credentials_title` | Credentials Title | Name of the credentials set.                                                                            | string | Required, `"API Key"`  |
| `/site`                          | Site              | The site prefix for your Chargebee instance (e.g., 'mycompany' for 'mycompany.chargebee.com').          | string | Required               |
| `/start_date`                    | Start Date        | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required, `30 day ago` |
| `/product_catalog`               | Product Catalog   | Product Catalog version of your Chargebee site (1.0 or 2.0).                                            | string | Required, `1.0`        |

#### Bindings

| Property        | Title         | Description                                                   | Type   | Required/Default |
| --------------- | ------------- | ------------------------------------------------------------- | ------ | ---------------- |
| **`/name`**     | Resource Name | Name of the Chargebee resource to capture.                    | string | Required         |
| **`/interval`** | Sync Interval | Interval between data syncs (e.g., PT2M for every 2 minutes). | string |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-chargebee-native:dev
        config:
          credentials:
            credentials_title: API Key
            api_key: <secret>
          site: mycompany
          start_date: "2024-01-01T00:00:00Z"
          product_catalog: "2.0"
    bindings:
      - resource:
          name: customers
        target: ${PREFIX}/customers
      - resource:
          name: subscriptions
        target: ${PREFIX}/subscriptions
```

## Resource Notes

* Some resources (Quotes and Quote Line Groups) require a Performance or Enterprise Chargebee subscription plan.
