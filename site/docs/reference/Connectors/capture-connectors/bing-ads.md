
# Bing Ads

This connector captures data from Bing Ads into Flow collections via the [Bing Ads API](https://learn.microsoft.com/en-us/advertising/guides/?view=bingads-13).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-bing-ads:dev`](https://ghcr.io/estuary/source-bing-ads:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

* [Accounts](https://learn.microsoft.com/en-us/advertising/customer-management-service/searchaccounts?view=bingads-13)
* [Account performance reports](https://learn.microsoft.com/en-us/advertising/reporting-service/accountperformancereportrequest?view=bingads-13): hourly, daily, weekly, and monthly (**four resources**)
* [Ad groups](https://learn.microsoft.com/en-us/advertising/campaign-management-service/getadgroupsbycampaignid?view=bingads-13)
* [Ad group performance reports](https://learn.microsoft.com/en-us/advertising/reporting-service/adgroupperformancereportrequest?view=bingads-13): hourly, daily, weekly, and monthly (**four resources**)
* [Ads](https://learn.microsoft.com/en-us/advertising/campaign-management-service/getadsbyadgroupid?view=bingads-13)
* [Ad performance reports](https://learn.microsoft.com/en-us/advertising/reporting-service/adperformancereportrequest?view=bingads-13): hourly, daily, weekly, and monthly (**four resources**).
* [Budget summary report](https://learn.microsoft.com/en-us/advertising/reporting-service/budgetsummaryreportrequest?view=bingads-13)
* [Campaigns](https://learn.microsoft.com/en-us/advertising/campaign-management-service/getcampaignsbyaccountid?view=bingads-13)
* [Campaign performance reports](https://learn.microsoft.com/en-us/advertising/reporting-service/campaignperformancereportrequest?view=bingads-13): hourly, daily, weekly, and monthly (**four resources**).
* [Keyword performance reports](https://learn.microsoft.com/en-us/advertising/reporting-service/keywordperformancereportrequest?view=bingads-13): hourly, daily, weekly, and monthly (**four resources**).

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

This connector uses OAuth2 to authenticate with Microsoft. You can do this in the Flow web app, or configure manually if you're using the flowctl CLI.

### Using OAuth2 to authenticate with Microsoft in the Flow web app

You'll need:

* User credentials with [access](https://help.ads.microsoft.com/#apex/3/en/52037/3-500) to the Bing Ads account.

* A [developer token](https://docs.microsoft.com/en-us/advertising/guides/get-started?view=bingads-13#get-developer-token) associated with the user.

### Authenticating manually using the CLI

You'll need:

* A registered Bing Ads application with the following credentials retrieved:

   * Client ID

   * Client Secret

   * Refresh Token

To set get these items, complete the following steps:

1. [Register your Bing Ads Application](https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-register?view=bingads-13) in the Azure Portal.

   1. During setup, note the `client_id` and `client_secret`.

2. Get a [user access token](https://learn.microsoft.com/en-us/advertising/guides/get-started?view=bingads-13#access-token).

   1. [Redeem the user authorization code for OAuth tokens](https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-get-tokens?view=bingads-13), and note the `refresh_token`.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Bing Ads source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-microsoft-in-the-flow-web-app),
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Credentials |  | object | Required |
| **`/credentials/auth_method`** | Authentication method | Set to `oauth2.0` | String | `oauth2.0` |
| **`/credentials/client_id`** | Client ID | The Client ID of your Microsoft Advertising developer application. | String | Required |
| **`/credentials/client_secret`** | Client Secret | The Client Secret of your Microsoft Advertising developer application. | String | Required |
| **`/credentials/refresh_token`** | Refresh Token | Refresh Token to renew the expired Access Token. | String | Required |
| **`/developer_token`** | Developer Token | Developer token associated with user. | String | Required |
| `/lookback_window` | Lookback Window | The number of days to "lookback" and re-capture data for performance report streams. This setting is typically used to capture late arriving conversions. | Integer | 0 |
| **`/reports_start_date`** | Credentials | The start date from which to begin replicating report data. Any data generated before this date will not be replicated in reports. This is a UTC date in YYYY-MM-DD format. | String | Required, `2020-01-01` |
| **`/tenant_id`** | Credentials | The Tenant ID of your Microsoft Advertising developer application. Set this to `common` unless you know you need a different value. | String | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Bing Ads resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample


This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-bing-ads:dev
          config:
            credentials:
              auth_type: oauth2.0
              client_id: 6731de76-14a6-49ae-97bc-6eba6914391e
              client_secret: <secret>
              refresh_token: <token>
            developer_token: <token>
            lookback_window: 0
            reports_start_date: 2020-01-01
            tenant_id: common

      bindings:
        - resource:
            stream: accounts
            syncMode: full_refresh
          target: ${PREFIX}/accounts
       {}
```
