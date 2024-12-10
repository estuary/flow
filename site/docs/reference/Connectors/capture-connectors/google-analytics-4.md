
# Google Analytics 4

This connector captures data from Google Analytics 4 properties into Flow collections via the
[Google Analytics Data API](https://developers.google.com/analytics/devguides/reporting/data/v1).

:::info
This connector supports Google Analytics 4, not Universal Analytics.

Universal Analytics is supported by a [separate connector](./google-analytics.md).
:::

It’s available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-google-analytics-data-api:dev`](https://ghcr.io/estuary/source-google-analytics-data-api:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.


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

There are two ways to authenticate this connector with Google:

* **Directly with Google using OAuth** through the Flow web app. You'll only need your username and password.

* **Manually**, by generating a service account key. Using this method, there are more prerequisites.

### Authenticating manually with a service account key

In addition to the above prerequisites, you'll need a Google service account with:

  * A JSON key generated.

  * Access to the Google Analytics 4 property.

To set this up:

1. Create a [service account and generate a JSON key](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount).
During setup, grant the account the **Viewer** role on your project.
You'll copy the contents of the downloaded key file into the Service Account Credentials parameter when you configure the connector.

2. [Add the service account](https://support.google.com/analytics/answer/9305788#zippy=%2Cin-this-article) to the Google Analytics property.

   1. Grant the account **Viewer** permissions.

## Configuration

You configure connectors either in the Flow web app, or by directly editing a specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Google Analytics 4 source connector.

### Properties

#### Endpoint

The following properties reflect the manual authentication method. If you authenticate directly with Google in the Flow web app, some of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials | Credentials for the service | object |  |
| `/credentials/auth_type` | Authentication Method | Set to `Service` for manual authentication. | string |  |
| `/credentials/credentials_json` | Service Account Credentials | Contents of the JSON key file generated during setup. | string |  |
| `/custom_reports` | Custom Reports (Optional) | A JSON array describing the custom reports you want to sync from Google Analytics. [Learn more about custom reports](#custom-reports).| string |  |
| **`/date_ranges_start_date`** | Date Range Start Date | The start date. One of the values `<N>daysago`, `yesterday`, `today` or in the format `YYYY-MM-DD`. | string | Required |
| **`/property_id`** | Property ID | A Google Analytics GA4 property identifier whose events are tracked. | string | Required |
| `/window_in_days` | Data request time increment in days (Optional) | The time increment used by the connector when requesting data from the Google Analytics API. We recommend setting this to 1 unless you have a hard requirement to make the sync faster at the expense of accuracy. The minimum allowed value for this field is 1, and the maximum is 364. See [data sampling](#data-sampling) for details. | integer | `1` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Data resource from Google Analytics. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. Always set to `incremental`. | string | Required |

### Custom reports

You can include data beyond the [default data resources](#supported-data-resources) with Custom Reports.
These replicate the functionality of [Custom Reports](https://support.google.com/analytics/answer/10445879?hl=en) in the Google Analytics Web console.

Fill out the Custom Reports property with a JSON array as a string with the following schema:

```json
[{"name": "<report-name>", "dimensions": ["<dimension-name>", ...], "metrics": ["<metric-name>", ...]}]
```

[Segments](https://support.google.com/analytics/answer/9304353#zippy=%2Cin-this-article) and [filters](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/FilterExpression#Filter) are also supported.
When using segments, you must include the `ga:segment` dimension:

```json
[{"name": "<report-name>", "dimensions": ["ga:segment", "<other-dimension-name>", ...], "metrics": ["<metric-name>", ...], "segments": "<segment-id>", "filter": "<filter-expression>"}]
```

### Sample

This sample reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-analytics-data-api:dev
          config:
            credentials:
              auth_type: Service
              credentials_json: <secret>
            date_ranges_start_date: 2023-01-01
            property_id: 000000000
            window_in_days: 1

      bindings:
        - resource:
            stream: daily_active_users
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: devices
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: four_weekly_active_users
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: locations
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: pages
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: traffic_sources
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: website_overview
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}

        - resource:
            stream: weekly_active_users
            syncMode: incremental
          target: ${PREFIX}/${COLLECTION_NAME}
```

## Performance considerations

### Data sampling

The Google Analytics Data API enforces compute thresholds for ad-hoc queries and reports.
If a threshold is exceeded, the API will apply sampling to limit the number of sessions analyzed for the specified time range.
These thresholds can be found [here](https://support.google.com/analytics/answer/2637192?hl=en&ref_topic=2601030&visit_id=637868645346124317-2833523666&rd=1#thresholds&zippy=%2Cin-this-article).

If your account is on the Analytics 360 tier, you're less likely to run into these limitations.
For Analytics Standard accounts, you can avoid sampling by keeping the `window_in_days` parameter set to its default value, `1`.
This makes it less likely that you will exceed the threshold.
