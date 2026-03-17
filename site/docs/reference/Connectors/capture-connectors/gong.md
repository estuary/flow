# Gong

This connector captures data from Gong into Estuary collections.

[Gong](https://www.gong.io/) is a revenue intelligence platform that captures and analyzes customer interactions across calls, emails, and meetings to provide insights for sales teams.

## Supported data resources

The following data resources are supported through the [Gong API](https://gong.app.gong.io/settings/api/documentation):

* [calls](https://gong.app.gong.io/settings/api/documentation#get-/v2/calls)
* [users](https://gong.app.gong.io/settings/api/documentation#post-/v2/users/extensive)
* [scorecards](https://gong.app.gong.io/settings/api/documentation#post-/v2/stats/activity/scorecards)
* [scorecard_definitions](https://gong.app.gong.io/settings/api/documentation#get-/v2/settings/scorecards)

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

* A Gong account with API access enabled.
* A Gong [API key](https://gong.app.gong.io/settings/api/documentation) consisting of an access key and access key secret. To generate API credentials, navigate to your Gong settings and create a new API key pair.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Gong source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_key`** | Access Key | Gong API Access Key. | string | Required |
| **`/credentials/access_key_secret`** | Access Key Secret | Gong API Access Key Secret. | string | Required |
| `/region` | Region | API region for your Gong account. | string | Default: `us-55616` |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Data generated before this date will not be replicated. If left blank, defaults to 30 days before the current date. | string | |
| `/calls_lookback_window` | Calls Lookback Window | Number of days to look back for calls that may have been enriched after the initial sync. Must be between 1 and 30. | integer | Default: `7` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Resource in Gong from which collections are captured. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gong:v1
        config:
          credentials:
            access_key: <secret>
            access_key_secret: <secret>
          region: us-55616
          start_date: 2024-01-01T00:00:00Z
    bindings:
      - resource:
          name: calls
        target: ${PREFIX}/calls
      - resource:
          name: users
        target: ${PREFIX}/users
      - resource:
          name: scorecards
        target: ${PREFIX}/scorecards
      - resource:
          name: scorecard_definitions
        target: ${PREFIX}/scorecard_definitions
```
