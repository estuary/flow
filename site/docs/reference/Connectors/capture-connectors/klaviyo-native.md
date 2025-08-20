# Klaviyo

This connector captures data from Klaviyo into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-klaviyo-native:dev`](https://ghcr.io/estuary/source-klaviyo-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Klaviyo API:

* [Campaigns](https://developers.klaviyo.com/en/reference/get_campaigns)
* [Coupon Codes](https://developers.klaviyo.com/en/reference/get_coupon_codes)
* [Coupons](https://developers.klaviyo.com/en/reference/get_coupons)
* [Flows](https://developers.klaviyo.com/en/reference/get_flows)
* [Forms](https://developers.klaviyo.com/en/reference/get_forms)
* [Events](https://developers.klaviyo.com/en/reference/get_events)
* [Images](https://developers.klaviyo.com/en/reference/get_images)
* [Lists](https://developers.klaviyo.com/en/reference/get_lists)
* [Metrics](https://developers.klaviyo.com/en/reference/get_metrics)
* [Profiles](https://developers.klaviyo.com/en/reference/get_profiles)
* [Push Tokens](https://developers.klaviyo.com/en/reference/get_push_tokens)
* [Segments](https://developers.klaviyo.com/en/reference/get_segments)
* [Tag Groups](https://developers.klaviyo.com/en/reference/get_tag_groups)
* [Tags](https://developers.klaviyo.com/en/reference/get_tags)
* [Templates](https://developers.klaviyo.com/en/reference/get_templates)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To set up the Klaviyo source connector, you'll need a [Klaviyo Private API key](https://help.klaviyo.com/hc/en-us/articles/115005062267-How-to-Manage-Your-Account-s-API-Keys#your-private-api-keys3).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Klaviyo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/start_date` | Start Date | The date from which you&#x27;d like to replicate data for Klaviyo API, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated. | string | 30 days before the present |
| **`/credentials`** | Credentials | Credentials for the service | object |  |
| **`/credentials/credentials_title`** | Authentication Method | Set to `API Key`. | string | Required |
| **`/credentials/access_token`** | API Key | The value of your Klaviyo private API Key. | string | Required |
| `/advanced/window_size` | Window Size | Date window size for the `events` backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours. If you have a significant amount of `events` data to backfill, smaller window sizes will allow the connector to checkpoint its progress more frequently. | string | P30D |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | 5M |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-klaviyo-native:dev
        config:
            advanced:
              window_size: P10D
            credentials:
              credentials: API Key
              access_token: <secret>
            start_date: "2025-08-14T00:00:00Z"
    bindings:
      - resource:
          name: events
        target: ${PREFIX}/events
```
