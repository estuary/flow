
# Google Analytics UA

This connector captures data from a view in Google Universal Analytics.

:::info
This connector supports Universal Analytics, not Google Analytics 4.

Google Analytics 4 is supported by a [separate connector](./google-analytics-4.md).
:::

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-google-analytics-ua:dev`](https://ghcr.io/estuary/source-google-analytics-ua:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/google-analytics-universal-analytics),
but keep in mind that the two versions may be significantly different.

## Supported data resources

The following data resources are captured to Flow collections by default:

* Website overview
* Traffic sources
* Pages
* Locations
* Monthly active users
* Four weekly active users
* Two weekly active users
* Weekly active users
* Daily active users
* Devices

Each resource is mapped to a Flow collection through a separate binding.

You can also configure [custom reports](#custom-reports).

## Prerequisites

There are two ways to authenticate with Google when capturing data from a Google Analytics view: using OAuth2, and manually, by generating a service account key.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the service account key method is the only supported method using the command line.

### Using OAuth2 to authenticate with Google in the Flow web app

* The View ID for your Google Analytics account.
You can find this using Google's [Account Explorer](https://ga-dev-tools.web.app/account-explorer/) tool.

* Your Google account username and password.

### Authenticating manually with a service account key

* The View ID for your Google Analytics account.
You can find this using Google's [Account Explorer](https://ga-dev-tools.web.app/account-explorer/) tool.

* Google Analytics and Google Analytics Reporting APIs enabled on your Google account.

* A Google service account with:
  * A JSON key generated.
  * Access to the source Google Analytics view.

Follow the steps below to meet these prerequisites:

1. [Enable](https://support.google.com/googleapi/answer/6158841?hl=en) the Google Analytics and Google Analytics Reporting APIs
for the Google [project](https://cloud.google.com/storage/docs/projects) with which your Analytics view is associated.
(Unless you actively develop with Google Cloud, you'll likely just have one option).

2. Create a [service account and generate a JSON key](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount)
During setup, grant the account the **Viewer** role on your project.
You'll copy the contents of the downloaded key file into the Service Account Credentials parameter when you configure the connector.

3. [Add the service account](https://support.google.com/analytics/answer/1009702#Add&zippy=%2Cin-this-article) to the Google Analytics view.
   1. Grant the account **Viewer** permissions (formerly known as Read & Analyze permissions).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.
The values and specification sample below provide configuration details specific to the Google Analytics source connector.

### Properties

#### Endpoint

The following properties reflect the Service Account Key authentication method. If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-google--in-the-flow-web-app), so some of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials | Credentials for the service | object |  |
| `/credentials/auth_type` | Authentication Type | Authentication method. Set to `Service` for manual configuration, or use OAuth in the web app. | string | Required |
| `credentials/credentials_json` | Service Account Credentials | Contents of the JSON key file generated during setup. | string | Required |
| `/custom_reports` | Custom Reports (Optional) | A JSON array describing the custom reports you want to sync from GA.  | string |  |
| **`/start_date`** | Start Date | The date in the format YYYY-MM-DD. Any data before this date will not be replicated. | string | Required |
| **`/view_id`** | View ID | The ID for the Google Analytics View you want to fetch data from. This can be found from the Google Analytics Account Explorer: https:&#x2F;&#x2F;ga-dev-tools.appspot.com&#x2F;account-explorer&#x2F; | string | Required |
| `/window_in_days` | Window in days (Optional) | The amount of days each stream slice would consist of beginning from start&#x5F;date. Bigger the value - faster the fetch. (Min=1, as for a Day; Max=364, as for a Year). | integer | `1` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Data resource from the Google Analytics view. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. Always set to `incremental`. | string | Required |

### Custom reports

You can include data beyond the [default data resources](#supported-data-resources) with Custom Reports.
These replicate the functionality of [Custom Reports](https://support.google.com/analytics/answer/10445879?hl=en) in the Google Analytics Web console.

To do so, fill out the Custom Reports property with a JSON array as a string with the following schema:

```json
[{"name": string, "dimensions": [string], "metrics": [string]}]
```

You may specify [default Google Analytics dimensions and metrics](https://ga-dev-tools.web.app/dimensions-metrics-explorer/) from the table below,
or custom dimensions and metrics you've previously defined.
Each custom report may contain up to 7 unique dimensions and 10 unique metrics.
You must include the `ga:date` dimension for proper data flow.

| Supported GA dimensions | Supported GA metrics |
|---|---|
| `ga:browser` | `ga:14dayUsers` |
| `ga:city` | `ga:1dayUsers` |
| `ga:continent` | `ga:28dayUsers` |
| `ga:country` | `ga:30dayUsers` |
| `ga:date` | `ga:7dayUsers` |
| `ga:deviceCategory` | `ga:avgSessionDuration` |
| `ga:hostname` | `ga:avgTimeOnPage` |
| `ga:medium` | `ga:bounceRate` |
| `ga:metro` | `ga:entranceRate` |
| `ga:operatingSystem` | `ga:entrances` |
| `ga:pagePath` | `ga:exits` |
| `ga:region` | `ga:newUsers` |
| `ga:socialNetwork` | `ga:pageviews` |
| `ga:source` | `ga:pageviewsPerSession` |
| `ga:subContinent` | `ga:sessions` |
|  | `ga:sessionsPerUser` |
|  | `ga:uniquePageviews` |
|  | `ga:users` |

### Sample

This sample reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-analytics-v4:dev
          config:
            view_id: 000000000
            start_date: 2022-03-01
            credentials:
              auth_type: service
              credentials_json: <secret>
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
            stream: monthly_active_users
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
            stream: two_weekly_active_users
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

[Learn more about capture definitions.](../../../concepts/captures.md)

## Performance considerations

### Data sampling

The Google Analytics Reporting API enforces compute thresholds for ad-hoc queries and reports.
If a threshold is exceeded, the API will apply sampling to limit the number of sessions analyzed for the specified time range.
These thresholds can be found [here](https://support.google.com/analytics/answer/2637192?hl=en&ref_topic=2601030&visit_id=637868645346124317-2833523666&rd=1#thresholds&zippy=%2Cin-this-article).

If your account is on the Analytics 360 tier, you're less likely to run into these limitations.
For Analytics Standard accounts, you can avoid sampling by keeping the `window_in_days` parameter set to its default value, `1`.
This makes it less likely that you will exceed the threshold.
When sampling occurs, a warning is written to the capture log.

### Processing latency

Data in Google Analytics reports may continue to update [up to 48 hours after it appears](https://support.google.com/analytics/answer/1070983?hl=en#DataProcessingLatency&zippy=%2Cin-this-article).

To ensure data correctness, each time it reads from Google Analytics,
this connector automatically applies a lookback window of 2 days prior to its last read.
This allows it to double-check and correct for any changes in reports resulting from latent data updates.

This mechanism relies on the `isDataGolden` flag in the [Google Analytics Reporting API](https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#reportdata).
