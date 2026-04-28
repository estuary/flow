# AppsFlyer

This connector captures data from [AppsFlyer](https://www.appsflyer.com/) into Estuary collections.

AppsFlyer is a mobile marketing analytics platform that provides app install attribution, in-app event tracking, and marketing campaign measurement. The connector supports two data collection modes:

- **Webhook mode**: Real-time event capture from AppsFlyer postback webhooks.
- **Pull API mode**: Historical and incremental data via AppsFlyer's Aggregate Report APIs.

## Supported data resources

The connector captures the following AppsFlyer resources:

**Webhook streams** via the [Push API](https://support.appsflyer.com/hc/en-us/articles/207034356-Push-API-streaming-raw-data):

- in-app-event
- install
- install-in-app-event
- organic-install
- organic-install-in-app-event
- organic-reinstall
- postback
- postbacks-copy
- re-attribution
- re-attribution-in-app-event
- re-download
- re-engagement
- re-engagement-in-app-event
- reinstall

**Pull API streams**:

- [daily_geo_aggregate_report](https://dev.appsflyer.com/hc/reference/get_app-id-geo-by-date-report-v5-1)

## Prerequisites

To set up the AppsFlyer connector, you need the following:

- An AppsFlyer account with API access.
- An AppsFlyer API V2 token. You can generate this from the [AppsFlyer dashboard](https://hq1.appsflyer.com/) under **Security Center > API Tokens**.
- One or more AppsFlyer App IDs for the apps you want to capture data from.

## Setup

Follow the steps below to set up the AppsFlyer connector.

1. Obtain your API V2 token from the AppsFlyer [dashboard](https://hq1.appsflyer.com/).
2. Identify the App IDs for the apps you want to capture data from.

### Set up webhook streaming

To receive real-time events from AppsFlyer, you need to configure AppsFlyer to send postbacks to the endpoint URL provided by Estuary:

1. First, publish your AppsFlyer capture in Estuary.
2. After publishing, go to the **Capture Details** page and scroll down to the **Endpoints** section. Copy the endpoint URL provided by Estuary.
3. In the [AppsFlyer dashboard](https://hq1.appsflyer.com/), navigate to **Integration > API Access** and configure your [Push API postback URLs](https://support.appsflyer.com/hc/en-us/articles/207034356-Push-API-streaming-raw-data) to point to the Estuary endpoint URL.
4. Configure your Push API export to include the following required fields, which Estuary uses to construct each document's unique identifier:
   - `app_id`
   - `app_type`
   - `appsflyer_id`
   - `campaign_type`
   - `conversion_type`
   - `event_name`
   - `event_time`
   - `event_value`

Once configured, AppsFlyer will stream events to Estuary in real time.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the AppsFlyer source connector.

### Properties

#### Endpoint

| Property                        | Title        | Description                                                                                    | Type   | Required/Default |
| ------------------------------- | ------------ | ---------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/app_ids`**                  | App IDs      | Comma-delimited list of AppsFlyer App IDs to capture data for.                                 | string | Required         |
| **`/start_date`**               | Start Date   | Start date for historical data replication in UTC format.                                      | string | Required         |
| **`/credentials`**              | Credentials  | Credentials for authenticating with the AppsFlyer API.                                         | object | Required         |
| **`/credentials/access_token`** | Access Token | AppsFlyer API V2 token.                                                                        | string | Required         |
| `/advanced/window_size`         | Window Size  | Window size for incremental syncs in ISO 8601 duration format (e.g., `P7D`). Range: 1-90 days. | string | P7D              |

#### Bindings

| Property        | Title     | Description                                                             | Type   | Required/Default |
| --------------- | --------- | ----------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your AppsFlyer account from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                      | string | Required         |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-appsflyer:v1
        config:
          app_ids: com.example.app
          start_date: 2025-01-01T00:00:00.000Z
          credentials:
            access_token: <your-api-token>
    bindings:
      - resource:
          stream: daily_geo_aggregate_report
          syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```
