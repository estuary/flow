
# WooCommerce
This connector captures data from WooCommerce into Estuary collections.

## Prerequisites
To set up the WooCommerce source connector you need:

* WooCommerce 3.5+
* WordPress 4.4+
* Pretty permalinks in Settings > Permalinks so that the custom endpoints are supported. e.g. /%year%/%monthnum%/%day%/%postname%/
* A new API key with read permissions and access to Customer key and Customer Secret.

## Setup
Follow the steps below to set up the WooCommerce source connector.

### Set up WooCommerce
1. Generate a new Rest API key.
2. Obtain Customer key and Customer Secret.


### Set up the WooCommerce connector in Estuary

1. Log into your Estuary account.
2. In the left navigation bar, click on "Captures". In the top-left corner, click "Connector Search".
3. Enter the name for the WooCommerce connector and select "WooCommerce" from the dropdown.
4. Fill in "Customer key" and "Customer Secret" with the data from Step 1 of this guide.
5. Fill in "Shop Name". For example, if your shop URL is https://EXAMPLE.com, the shop name is 'EXAMPLE.com'.
6. Choose the start date you want to start syncing data from.

## Configuration
You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the WooCommerce source connector.

### Properties

#### Endpoint
| Property           | Title           | Description                                                        | Type   | Required/Default |
| ------------------ | --------------- | ------------------------------------------------------------------ | ------ | ---------------- |
| `/api_key`    | Customer Key    | Customer Key for API in WooCommerce shop                           | string | Required         |
| `/api_secret` | Customer Secret | Customer Secret for API in WooCommerce shop                        | string | Required         |
| `/shop`       | Shop Name       | The name of the store.                                             | string | Required         |
| `/start_date`      | Start Date      | The date you would like to replicate data from. Format: YYYY-MM-DD | string | Required         |


#### Bindings

| Property        | Title     | Description                                                               | Type   | Required/Default |
| --------------- | --------- | ------------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your WooCommerce project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                        | string | Required         |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-woocommerce:v1
        config:
          api_key: api-key
          api_secret: secret
          shop: acmeCo
          start_date: 2025-01-01
    bindings:
      - resource:
          stream: orders
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```

## Supported Streams
The WooCommerce source connector in Estuary supports the following streams:

* Coupons (Incremental)
* Customers (Incremental)
* Orders (Incremental)
* Order notes
* Payment gateways
* Product attribute terms
* Product attributes
* Product categories
* Product reviews (Incremental)
* Product shipping classes
* Product tags
* Product variations
* Products (Incremental)
* Refunds
* Shipping methods
* Shipping zone locations
* Shipping zone methods
* Shipping zones
* System status tools
* Tax classes
* Tax rates

## Connector-Specific Features & Highlights
Useful links:

[WooCommerce Rest API Docs](https://woocommerce.github.io/woocommerce-rest-api-docs/#introduction).
