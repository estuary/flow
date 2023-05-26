---
sidebar_position: 1
---
# YouTube Analytics

This connector captures data from YouTube Analytics into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-youtube-analytics:dev`](https://ghcr.io/estuary/source-youtube-analytics:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/youtube-analytics/),
but keep in mind that the two versions may be significantly different.

## Supported data resources

The following data resources are supported through the YouTube Analytics APIs:

* [channel_annotations_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-annotations)
* [channel_basic_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-user-activity)
* [channel_cards_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-cards)
* [channel_combined_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-combined)
* [channel_demographics_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-viewer-demographics)
* [channel_device_os_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-device-type-and-operating-system)
* [channel_end_screens_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-end-screens)
* [channel_playback_location_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-playback-locations)
* [channel_province_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-province)
* [channel_sharing_service_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-content-sharing)
* [channel_subtitles_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-subtitles)
* [channel_traffic_source_a2](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#video-traffic-sources)
* [playlist_basic_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#playlist-user-activity)
* [playlist_combined_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#playlist-combined)
* [playlist_device_os_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#playlist-device-type-and-operating-system)
* [playlist_playback_location_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#playlist-playback-locations)
* [playlist_province_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#playlist-province)
* [playlist_traffic_source_a1](https://developers.google.com/youtube/reporting/v1/reports/channel_reports#playlist-traffic-sources)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* YouTube does not start to generate a report until you create a [reporting job](https://developers.google.com/youtube/reporting/v1/reports#step-3:-create-a-reporting-job) for that report.
Airbyte creates a reporting job for your report or uses current reporting job if it's already exists.
The report will be available within 48 hours of creating the reporting job and will be for the day that the job was scheduled.
For example, if you schedule a job on September 1, 2015, then the report for September 1, 2015, will be ready on September 3, 2015.
The report for September 2, 2015, will be posted on September 4, 2015, and so forth.
Youtube also generates historical data reports covering the 30-day period prior to when you created the job. Airbyte syncs all available historical data too.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the YouTube Analytics source connector.

### Properties

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your YouTube Analytics project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-youtube-analytics:dev
        config:
          credentials:
            auth_type: OAuth
    bindings:
      - resource:
          stream: channel_annotations_a1
          syncMode: incremental
        target: ${PREFIX}/channel_annotations_a1
      {...}
```
