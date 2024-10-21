# Snapchat Marketing

This connector captures data from Snapchat Marketing into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-snapchat:dev`](https://ghcr.io/estuary/source-snapchat:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

This connector can be used to sync the following tables from Snapchat:

* Adaccounts
* Ads
* Adsquads
* Campaigns
* Creatives
* Media
* Organizations
* Segments
* AdaccountsStatsHourly
* AdaccountsStatsDaily
* AdaccountsStatsLifetime
* AdsStatsHourly
* AdsStatsDaily
* AdsStatsHourly
* AdsStatsDaily
* AdsStatsLifetime
* AdsquadsStatsDaily
* AdsquadsStatsLifetime
* CampaignsStatsHourly
* CampaignsStatsDaily
* CampaignsStatsLifetime

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Snapchat Marketing account with permission to access data from accounts you want to sync.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Snapchat source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Default |
| `/end_date` | End Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Snapchat project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-snapchat:dev
        config:
          start_date: 2017-01-25T00:00:00Z
          end_date: 2018-01-25T00:00:00Z
    bindings:
      - resource:
          stream: lists
          syncMode: full_refresh
        target: ${PREFIX}/lists
      {...}
```