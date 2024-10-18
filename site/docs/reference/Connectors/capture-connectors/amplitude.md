
# Amplitude

This connector captures data from Amplitude into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-amplitude:dev`](https://ghcr.io/estuary/source-amplitude:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Amplitude APIs:

* [Active User Counts](https://developers.amplitude.com/docs/dashboard-rest-api#active-and-new-user-counts)
* [Annotations](https://developers.amplitude.com/docs/chart-annotations-api#get-all-annotations)
* [Average Session Length](https://developers.amplitude.com/docs/dashboard-rest-api#average-session-length)
* [Cohorts](https://developers.amplitude.com/docs/behavioral-cohorts-api#listing-all-cohorts)
* [Events](https://developers.amplitude.com/docs/export-api#export-api---export-your-projects-event-data)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* An Amplitude project with an [API Key and Secret Key](https://help.amplitude.com/hc/en-us/articles/360058073772-Create-and-manage-organizations-and-projects)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Amplitude source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/api_key`** | API Key | Amplitude API Key. | string | Required |
| **`/secret_key`** | Secret Key | Amplitude Secret Key. | string | Required |
| **`/start_date`** | Replication Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Amplitude project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-amplitude:dev
        config:
            api_key: <secret>
            secret_key: <secret>
            start_date: 2022-06-18T00:00:00Z
    bindings:
      - resource:
          stream: cohorts
          syncMode: full_refresh
        target: ${PREFIX}/cohorts
      - resource:
          stream: annotations
          syncMode: full_refresh
        target: ${PREFIX}/annotations
      - resource:
          stream: events
          syncMode: incremental
        target: ${PREFIX}/events
      - resource:
          stream: active_users
          syncMode: incremental
        target: ${PREFIX}/activeusers
      - resource:
          stream: average_session_length
          syncMode: incremental
        target: ${PREFIX}/averagesessionlength
```
