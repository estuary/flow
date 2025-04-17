
# Google Analytics Data API

This connector captures data from Google Analytics 4 properties into Flow collections via the
[Google Analytics Data API](https://developers.google.com/analytics/devguides/reporting/data/v1).

It’s available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-google-analytics-data-api-native:dev`](https://ghcr.io/estuary/source-google-analytics-data-api-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported:

* Daily active users
* Devices
* Four-weekly active users
* Locations
* Pages
* Traffic sources
* Website overview
* Weekly active users

Each is [fetched as a report](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport) and mapped to a Flow collection through a separate binding.

You can also capture [custom reports](#custom-reports).

## Prerequisites

To use this connector, you'll need:

* The Google Analytics Data API [enabled](https://support.google.com/googleapi/answer/6158841?hl=en) on your Google [project](https://cloud.google.com/storage/docs/projects) with which your Analytics property is associated.
(Unless you actively develop with Google Cloud, you'll likely just have one option).

* Your Google Analytics 4 [property ID](https://developers.google.com/analytics/devguides/reporting/data/v1/property-id#what_is_my_property_id).

## Authentication

Your Google username and password is required to authenticate the connector using OAuth2.

## Configuration

You configure connectors either in the Flow web app, or by directly editing a specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Google Analytics Data API source connector.

### Properties

#### Endpoint

The following properties reflect the manual authentication method. If you authenticate directly with Google in the Flow web app, some of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/property_id`** | Property ID | A Google Analytics GA4 property identifier whose events are tracked. | string | Required |
| `/custom_reports` | Custom Reports | A JSON array describing the custom reports you want to sync from Google Analytics. [Learn more about custom reports](#custom-reports).| string |  |
| `/start_date` | Start Date | The date from which you&#x27;d like to replicate data, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated. | string | Defaults to 30 days before the present |
| `/advanced/lookback_window_size` | Lookback window size | The number of days to lookback from the present for updates. | integer | 30 |
| `/credentials` | Credentials | Credentials for the service | object |  |
| `/credentials/credentials_title` | Authentication Method | Set to `OAuth Credentials`. | string | Required |
| `/credentials/client_id` | OAuth Client ID | The OAuth app's client ID. | string | Required |
| `/credentials/client_secret` | OAuth Client Secret | The OAuth app's client secret. | string | Required |
| `/credentials/refresh_token` | Refresh Token | The refresh token received from the OAuth app. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |    PT30M      |

### Custom reports

You can include data beyond the [default data resources](#supported-data-resources) with Custom Reports.
These replicate the functionality of [Custom Reports](https://support.google.com/analytics/answer/10445879?hl=en) in the Google Analytics Web console.

Fill out the Custom Reports property with a JSON array as a string with the following schema:

```json
[{"name": "<report-name>", "dimensions": ["<dimension-name>", ...], "metrics": ["<metric-name>", ...]}]
```

[Filters](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/FilterExpression#Filter) are also supported. See Google's documentation for [examples](https://developers.google.com/analytics/devguides/reporting/data/v1/basics#filter) of filters and valid [filter syntax](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/FilterExpression).

```json
[{"name": "<report-name>", "dimensions": ["<dimension-name>", ...], "metrics": ["<metric-name>", ...], "dimensionFilter": "<filter-object>", "metricFilter": "<another-filter-object>"}]
```

The `TOTAL`, `MAXIMUM`, and `MINIMUM` [metric aggregations](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/MetricAggregation) are supported as well. These aggregates will be emitted as separate documents with the dimension values indicating the type of aggregation, like `RESTRICTED_TOTAL`.

```json
[{"name": "<report-name>", "dimensions": ["<dimension-name>", ...], "metrics": ["<metric-name>", ...], "metricAggregations": ["TOTAL", "MAXIMUM", "MINIMUM"]}]
```

:::tip

After editing custom reports for a capture, always [re-discover](../../../concepts/captures.md#discovery) bindings to ensure the changes to your custom reports are reflected in the associated collections' specs and schemas.

:::

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-analytics-data-api-native:dev
          config:
            custom_reports: '[{"name": "my_custom_report_with_a_filter_and_aggregate", "dimensions": ["browser"], "metrics": ["totalUsers"], "dimensionFilter": {"filter": {"fieldName": "browser", "stringFilter": {"value": "Chrome"}}}, "metricAggregates": ["TOTAL"]}]'
            credentials:
                credentials_title: OAuth Credentials
                client_id: <secret>
                client_secret: <secret>
                refresh_token: <secret>
            start_date: "2025-02-07T17:00:00Z"
            property_id: "123456789"
            advanced:
                lookback_window_size: 30
      bindings:
        - resource:
            name: daily_active_users
            interval: PT30M
          target: ${PREFIX}/daily_active_users

```

## Performance considerations

### Data sampling

The Google Analytics Data API enforces compute thresholds for ad-hoc queries and reports.
If a threshold is exceeded, the API will apply sampling to limit the number of sessions analyzed for the specified time range.
These thresholds can be found [here](https://support.google.com/analytics/answer/2637192?hl=en&ref_topic=2601030&visit_id=637868645346124317-2833523666&rd=1#thresholds&zippy=%2Cin-this-article).
If your account is on the Analytics 360 tier, you're less likely to run into these limitations.
