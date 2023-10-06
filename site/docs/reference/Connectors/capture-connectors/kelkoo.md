# Kelkoo

This connector allows you to capture data from your Kelkoo account into Flow collections.

[`ghcr.io/estuary/source-kelkoo:dev`](https://ghcr.io/estuary/source-kelkoo:dev) provides the latest connector image. For accessing past image versions, follow the provided link in your browser.

## Supported Data Resources

The following data resources are supported through the Kelkoo APIs:

- [Campaigns](https://developers.kelkoogroup.com/app/documentation/navigate/_merchant/merchantStatistics/_/_/Resources#my-campaigns): Detailed information about your campaigns including internal identifiers, names, countries, and website URLs.
- [Clicks by Category](https://developers.kelkoogroup.com/app/documentation/navigate/_merchant/merchantStatistics/_/_/Resources#categorycampaignidstartdatestartenddateend): Click data grouped by categories for a specific campaign within a specified date range.
- [Clicks by Product](https://developers.kelkoogroup.com/app/documentation/navigate/_merchant/merchantStatistics/_/_/Resources#productcampaignidstartdatestartenddateend): Click data grouped by products for a specific campaign within a specified date range.
- [Sales](https://developers.kelkoogroup.com/app/documentation/navigate/_merchant/merchantStatistics/_/_/Resources#salescampaignidstartdatestartenddateend): Sales data for a specific campaign within a specified date range.

## Prerequisites

To use this connector, you'll need:

- `api_token`: API Token from Kelkoo. Refer to [this guide](https://developers.kelkoogroup.com/app/documentation/navigate/_merchant/merchantStatistics/_/_Guides/ManageTokens) to generate and manage tokens.
- `merchant_id`: Your Kelkoo Merchant ID.

## Configuration

The Kelkoo source connector configuration can be done through the Flow web app or by editing the Catalog specification file directly. Refer to our guide on [connectors](../../../concepts/connectors.md#using-connectors) for more information about using connectors. Below are the values and specification sample providing configuration details specific to the Kelkoo source connector.

### Properties

#### Endpoint

| Property        | Title      | Description                                              | Type   | Required/Default       |
|-----------------|------------|----------------------------------------------------------|--------|------------------------|
| **`/api_token`**| API Token  | Kelkoo API token.                             | string | Required               |
| **`/start_date`**| Start Date | Date time filter for incremental filter, specify which date to extract from. | string |               |
| **`/end_date`** | End Date   | Specify the end date of the data to be extracted. | string |    Note: Only two years of data can be retrieved.           |
| **`/merchant_id`** | Merchant ID | Kelkoo Merchant ID. | string | Required |

#### Bindings

| Property         | Title      | Description                                           | Type   | Required/Default       |
|------------------|------------|-------------------------------------------------------|--------|------------------------|
| **`/stream`**    | Stream     | Resource from which collections are captured. | string | Required               |
| **`/syncMode`**  | Sync Mode  | Connection method.                                   | string | Required               |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-kelkoo:dev
        config:
          api_token: <your-token>
          start_date: 2022-01-21T00:00:00Z
          end_date: 
          merchant_id: <id>
    bindings:
      - resource:
          stream: products
          syncMode: full_refresh
        target: ${PREFIX}/products
```
