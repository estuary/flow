# TikTok Marketing

This connector captures data from TikTok marketing campaigns and ads into Flow collections via the [TikTok API for Business](https://ads.tiktok.com/marketing_api/docs). It supports production as well as [sandbox](https://ads.tiktok.com/marketing_api/docs?id=1738855331457026) accounts.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-tiktok-marketing:dev`](https://ghcr.io/estuary/source-tiktok-marketing:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

| Resource | Production | Sandbox |
|---|---|---|
| Advertisers | X | X |
| Ad Groups | X | X |
| Ads | X | X |
| Campaigns | X | X |
| Ads Reports Hourly | X | X |
| Ads Reports Daily | X | X |
| Ads Reports Lifetime | X | X |
| Advertisers Reports Hourly | X | |
| Advertisers Reports Daily | X | |
| Advertisers Reports Lifetime | X | |
| Ad Groups Reports Hourly | X | X |
| Ad Groups Reports Daily | X | X |
| Ad Groups Reports Lifetime | X | X |
| Campaigns Reports Hourly | X | X |
| Campaigns Reports Daily | X | X |
| Campaigns Reports Lifetime | X | X |
| Advertisers Audience Reports Hourly | X | |
| Advertisers Audience Reports Daily | X | |
| Advertisers Audience Reports Lifetime | X | |
| Ad Group Audience Reports Hourly | X | X |
| Ad Group Audience Reports Daily | X | X |
| Ads Audience Reports Hourly | X | X |
| Ads Audience Reports Daily | X | X |
| Campaigns Audience Reports By Country Hourly | X | X |
| Campaigns Audience Reports By Country Daily | X | X |

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

Prerequisites differ depending on whether you have a production or [sandbox](https://ads.tiktok.com/marketing_api/docs?id=1738855331457026)
TikTok for Business account, and on whether you'll use the Flow web app or the flowctl CLI.

### OAuth authentication in the web app (production accounts)

If you have a TikTok marketing account in production and will use the Flow web app, you'll be able to quickly log in using OAuth.

You'll need:

* A [TikTok for Business account](https://ads.tiktok.com/marketing_api/docs?rid=fgvgaumno25&id=1702715936951297) with one or more active campaigns.

   * Note the username and password used to sign into this account

### Sandbox access token authentication in the web app or CLI

If you're working in a Sandbox TikTok for Business account, you'll authenticate with an access token in both the web app and CLI.

You'll need:

* A [TikTok for Business account](https://ads.tiktok.com/marketing_api/docs?rid=fgvgaumno25&id=1702715936951297).

* A [Sandbox account](https://ads.tiktok.com/marketing_api/docs?rid=fgvgaumno25&id=1701890920013825) created under an existing
 [developer application](https://ads.tiktok.com/marketing_api/docs?rid=fgvgaumno25&id=1702716474845185).

   * Generate an access token and note the advertiser ID for the Sandbox.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the TikTok Marketing source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method for Sandbox accounts.
If you're using a production account, you'll use [OAuth2](#oauth-authentication-in-the-web-app-production-accounts) to authenticate in the Flow web app,
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication Method | Authentication method | object | Required |
| **`/credentials/auth_type`** | Authentication type | Set to `sandbox_access_token` to manually authenticate a Sandbox. | string | Required |
| `/credentials/advertiser_id` | Advertiser ID | The Advertiser ID generated for the developer's Sandbox application. | string | |
| `/credentials/access_token` | Access Token | The long-term authorized access token. | string | |
| `/end_date` | End Date | The date until which you'd like to replicate data for all incremental streams, in the format YYYY-MM-DD. All data generated between `start_date` and this date will be replicated. Not setting this option will result in always syncing the data till the current date. | string | |
| `/report_granularity` | Report Aggregation Granularity | The granularity used for [aggregating performance data in reports](#report-aggregation). Choose `DAY`, `LIFETIME`, or `HOUR`.| string | |
| `/start_date` | Start Date | Replication Start Date | The Start Date in format: YYYY-MM-DD. Any data before this date will not be replicated. If this parameter is not set, all data will be replicated. | string | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | TikTok resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the access token method for Sandbox accounts.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-tiktok-marketing:dev
        config:
          credentials:
            auth_type: sandbox_access_token
            access_token: {secret}
            advertiser_id: {secret}
          end_date: 2022-01-01
          report_granularity: DAY
          start_date: 2020-01-01
    bindings:
      - resource:
          stream: campaigns
          syncMode: incremental
        target: ${PREFIX}/campaigns
      {...}
```

## Report aggregation

Many of the [resources](#supported-data-resources) this connector supports are reports.
Data in these reports is aggregated into rows based on the granularity you select in the [configuration](#endpoint).

You can choose hourly, daily, or lifetime granularity. For example, if you choose daily granularity, the report will contain one row for each day.