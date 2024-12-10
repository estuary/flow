
# Klaviyo

This connector captures data from Klaviyo into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-klaviyo:dev`](https://ghcr.io/estuary/source-klaviyo:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

This connector can be used to sync the following tables from Klaviyo:

* [Campaigns](https://developers.klaviyo.com/en/v1-2/reference/get-campaigns#get-campaigns)
* [Events](https://developers.klaviyo.com/en/v1-2/reference/metrics-timeline)
* [GlobalExclusions](https://developers.klaviyo.com/en/v1-2/reference/get-global-exclusions)
* [Lists](https://developers.klaviyo.com/en/v1-2/reference/get-lists)
* [Metrics](https://developers.klaviyo.com/en/v1-2/reference/get-metrics)
* [Flows](https://developers.klaviyo.com/en/reference/get_flows)
* [Profiles](https://developers.klaviyo.com/en/reference/get_profiles)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Klaviyo source connector, you'll need the [Klaviyo Private API key](https://help.klaviyo.com/hc/en-us/articles/115005062267-How-to-Manage-Your-Account-s-API-Keys#your-private-api-keys3).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Klaviyo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/api_key` | API Key | The value of the Klaviyo API Key generated. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Klaviyo project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-klaviyo:dev
        config:
          api_key: <secret>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: lists
          syncMode: full_refresh
        target: ${PREFIX}/lists
      {...}
```
