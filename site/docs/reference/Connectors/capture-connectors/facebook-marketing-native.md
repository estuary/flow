
# Facebook Marketing

This connector captures data from the Facebook Marketing API into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-facebook-marketing-native:dev`](https://ghcr.io/estuary/source-facebook-marketing-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported:

* [Ads](https://developers.facebook.com/docs/marketing-api/reference/adgroup)
* [Ad activities](https://developers.facebook.com/docs/marketing-api/reference/ad-activity)
* [Ad creatives](https://developers.facebook.com/docs/marketing-api/reference/ad-creative)
* [Ad insights](https://developers.facebook.com/docs/marketing-api/reference/adgroup/insights/)
* [Ad sets](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/v19.0)
* [Business ad accounts](https://developers.facebook.com/docs/marketing-api/reference/business/adaccount/)
* [Campaigns](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group)
* [Custom Conversions](https://developers.facebook.com/docs/marketing-api/reference/custom-conversion/v19.0)
* [Images](https://developers.facebook.com/docs/marketing-api/reference/ad-image)
* [Videos](https://developers.facebook.com/docs/graph-api/reference/video/)

By default, each resource associated with your Facebook Business account is mapped to a Flow collection through a separate binding.

## Prerequisites

There are two ways to authenticate with Facebook when capturing data into Flow: signing in with OAuth2, and manually supplying an access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the manual method is the only supported method using the command line.

### Signing in with OAuth2

To use OAuth2 in the Flow web app, you'll need a Facebook Business account and its [Ad Account ID](https://www.facebook.com/business/help/1492627900875762).

### Configuring manually with an access token

To configure manually with an access token, you'll need:

* A Facebook Business account, and its Ad Account ID.
* A Facebook app with:
  * The [Marketing API](https://developers.facebook.com/products/marketing-api/) enabled.
  * A Marketing API access token generated.
  * Access upgrade from Standard Access (the default) to Advanced Access. This allows a sufficient [rate limit](https://developers.facebook.com/docs/marketing-api/overview/authorization#limits) to support the connector.

Follow the steps below to meet these requirements.

#### Setup

1. Find your Facebook [Ad Account ID](https://www.facebook.com/business/help/1492627900875762).

2. In Meta for Developers, [create a new app](https://developers.facebook.com/docs/development/create-an-app/) of the type Business.

3. On your new app's dashboard, click the button to set up the Marketing API.

4. On the Marketing API Tools tab, generate a Marketing API access token with all available permissions (`ads_management`, `ads_read`, `read_insights`, and `business_management`).

5. [Request Advanced Access](https://developers.facebook.com/docs/marketing-api/overview/authorization/#access-levels) for your app. Specifically request the Advanced Access to the following:

   * The feature `Ads Management Standard Access`

   * The permission `ads_read`

   * The permission `ads_management`

   Once your request is approved, you'll have a high enough rate limit to proceed with running the connector.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Facebook Marketing source connector.

### Properties

#### Endpoint

By default, this connector captures all data associated with your Business Ad Account.

You can refine the data you capture from Facebook Marketing using the optional Custom Insights configuration.
You're able to specify certain fields to capture and apply data breakdowns.
[Breakdowns](https://developers.facebook.com/docs/marketing-api/insights/breakdowns) are a feature of the Facebook Marketing Insights API that allows you to group API output by common metrics.
[Action breakdowns](https://developers.facebook.com/docs/marketing-api/insights/breakdowns#actionsbreakdown)
are a subset of breakdowns that must be specified separately.

| Property                                        | Title                    | Description                                                                                                                                             | Type    | Required/Default                   |
| ----------------------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------------------------- |
| **`/credentials/credentials_title`**            | Authentication Method    | Set to `OAuth Credentials`.                                                                                                                             | string  | Required                           |
| **`/credentials/client_id`**                    | Client ID                | The client ID obtained from your Facebook app.                                                                                                          | string  | Required for OAuth2 authentication |
| **`/credentials/client_secret`**                | Client Secret            | The client secret obtained from your Facebook app.                                                                                                      | string  | Required for OAuth2 authentication |
| **`/account_ids`**                              | Account IDs              | A comma delimited string of Facebook Ad account IDs to use when pulling data from the Facebook Marketing API.                                           | string  | Required                           |
| `/custom_insights`                              | Custom Insights          | A list which contains insights entries.                                                                                                                 | array   |                                    |
| _`/custom_insights/-/action_breakdowns`_        | Action Breakdowns        | A comma separated string of chosen action&#x5F;breakdowns to apply                                                                                      | string  |                                    |
| _`/custom_insights/-/breakdowns`_               | Breakdowns               | A comma separated string of chosen breakdowns to apply                                                                                                  | string  |                                    |
| _`/custom_insights/-/fields`_                   | Fields                   | A comma separated string of chosen fields to capture                                                                                                    | string  |                                    |
| _`/custom_insights/-/name`_                     | Name                     | The name of the insight                                                                                                                                 | string  |                                    |
| _`/custom_insights/-/start_date`_               | Start Date               | The date from which you&#x27;d like to replicate data for this stream, in the format YYYY-MM-DDTHH:mm:ssZ.                                              | string  |                                    |
| _`/custom_insights/-/level`_                    | Level                    | The level of the insight. Possible values are `ad`, `adset`, `campaign`, or `account`.                                                                  | string  | `ad`                               |
| _`/custom_insights/-/insights_lookback_window`_ | Insights Lookback Window | The lookback window for custom insights.                                                                                                                | integer | `28`                               |
| `/insights_lookback_window`                     | Insights Lookback Window | The lookback window for insights.                                                                                                                       | integer | `28`                               |
| **`/start_date`**                               | Start Date               | The date from which you&#x27;d like to begin capturing data, in the format YYYY-MM-DDTHH:mm:ssZ. All data generated after this date will be replicated. | string  | Required                           |
| `/advanced/fetch_thumbnail_images`              | Fetch Thumbnail Images   | In each Ad Creative, fetch the thumbnail&#x5F;url and store the result in thumbnail&#x5F;data&#x5F;url                                                  | boolean | `false`                            |
| `/advanced/include_deleted`                     | Include Deleted          | Include data from deleted Campaigns, Ads, and AdSets                                                                                                    | boolean | `false`                            |

#### Bindings

| Property    | Title         | Description                 | Type   | Required/Default |
| ----------- | ------------- | --------------------------- | ------ | ---------------- |
| **`/name`** | Data resource | Name of the data resource.  | string | Required         |
| `/interval` | Interval      | Interval between data syncs | string |                  |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-facebook-marketing-native:dev
        config:
            account_ids: "111111111111111"
            credentials:
                credentials_title: OAuth Credentials
                client_id: placeholder_client_id
                client_secret: placeholder_client_secret
                access_token: placeholder_access_token
            start_date: "2023-03-24T00:00:00Z"
            custom_insights:
                - name: ads_insights_publisher_platform
                  action_breakdowns: ""
                  breakdowns: publisher_platform
                  fields: "ad_id,ad_name,account_id,account_name,adset_id,adset_name,campaign_id,campaign_name,date_start,date_stop,clicks,impressions,reach,inline_link_clicks,outbound_clicks"
                  insights_lookback_window: 28
                  level: ad
                  start_date: "2023-03-23T00:00:00Z"
            insights_lookback_window: 28
            advanced:
              fetch_thumbnail_images: false
              include_deleted: false
    bindings:
      - resource:
          name: ad_account
          interval: PT1H
        target: ${PREFIX}/ad_account
      - resource:
          name: ad_sets
          interval: PT1H
        target: ${PREFIX}/ad_sets
      - resource:
          name: ads_insights
          interval: PT1H
        target: ${PREFIX}/ads_insights
      - resource:
          name: ads_insights_age_and_gender
          interval: PT1H
        target: ${PREFIX}/ads_insights_age_and_gender
      - resource:
          name: ads_insights_country
          interval: PT1H
        target: ${PREFIX}/ads_insights_country
      - resource:
          name: ads_insights_region
          interval: PT1H
        target: ${PREFIX}/ads_insights_region
      - resource:
          name: ads_insights_dma
          interval: PT1H
        target: ${PREFIX}/ads_insights_dma
      - resource:
          name: ads_insights_platform_and_device
          interval: PT1H
        target: ${PREFIX}/ads_insights_platform_and_device
      - resource:
          name: ads_insights_action_type
          interval: PT1H
        target: ${PREFIX}/ads_insights_action_type
      - resource:
          name: campaigns
          interval: PT1H
        target: ${PREFIX}/campaigns
      - resource:
          name: custom_conversions
          interval: PT1H
        target: ${PREFIX}/custom_conversions
      - resource:
          name: activities
          interval: PT1H
        target: ${PREFIX}/activities
      - resource:
          name: ads
          interval: PT1H
        target: ${PREFIX}/ads
      - resource:
          name: ad_creatives
          interval: PT1H
        target: ${PREFIX}/ad_creatives
```

[Learn more about capture definitions.](../../../concepts/captures.md)
