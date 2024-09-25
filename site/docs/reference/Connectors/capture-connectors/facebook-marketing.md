
# Facebook Marketing

This connector captures data from the Facebook Marketing API into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-facebook-marketing:dev`](https://ghcr.io/estuary/source-facebook-marketing:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.


This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

* [Ads](https://developers.facebook.com/docs/marketing-api/reference/adgroup)
* [Ad activities](https://developers.facebook.com/docs/marketing-api/reference/ad-activity)
* [Ad creatives](https://developers.facebook.com/docs/marketing-api/reference/ad-creative)
* [Ad insights](https://developers.facebook.com/docs/marketing-api/reference/adgroup/insights/)
* [Business ad accounts](https://developers.facebook.com/docs/marketing-api/reference/business/adaccount/)
* [Campaigns](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group)
* [Images](https://developers.facebook.com/docs/marketing-api/reference/ad-image)
* [Videos](https://developers.facebook.com/docs/graph-api/reference/video/)

By default, each resource associated with your Facebook Business account is mapped to a Flow collection through a separate binding.

## Prerequisites

There are two ways to authenticate with Facebook when capturing data into Flow: signing in with OAuth2, and manually supplying an access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the manual method is the only supported method using the command line.

### Signing in with OAuth2

To use OAuth2 in the Flow web app, you'll need A Facebook Business account and its [Ad Account ID](https://www.facebook.com/business/help/1492627900875762).

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

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/access_token`** | Access Token | The value of the access token generated. | string | Required |
| **`/account_id`** | Account ID | The Facebook Ad account ID to use when pulling data from the Facebook Marketing API. | string | Required for [manual authentication](#configuring-manually-with-an-access-token) only |
| `/custom_insights` | Custom Insights | A list which contains insights entries. Each entry must have a name and can contains fields, breakdowns or action&#x5F;breakdowns | array |  |
| _`/custom_insights/-/action_breakdowns`_ | Action Breakdowns | A list of chosen action&#x5F;breakdowns to apply | array | `[]` |
| _`/custom_insights/-/action_breakdowns/-`_ | ValidActionBreakdowns | Generic enumeration. Derive from this class to define new enumerations. | string |  |
| _`/custom_insights/-/breakdowns`_ | Breakdowns | A list of chosen breakdowns to apply | array | `[]` |
| _`/custom_insights/-/breakdowns/-`_ | ValidBreakdowns | Generic enumeration. Derive from this class to define new enumerations. | string |  |
| _`/custom_insights/-/end_date`_ | End Date | The date until which you&#x27;d like to replicate data for this stream, in the format YYYY-MM-DDT00:00:00Z. All data generated between the start date and this date will be replicated. Not setting this option will result in always syncing the latest data. | string |  |
| _`/custom_insights/-/fields`_ | Fields | A list of chosen fields to capture | array | `[]` |
| _`/custom_insights/-/fields/-`_ | ValidEnums | Generic enumeration. Derive from this class to define new enumerations. | string |  |
| _`/custom_insights/-/name`_ | Name | The name of the insight | string |  |
| _`/custom_insights/-/start_date`_ | Start Date | The date from which you&#x27;d like to replicate data for this stream, in the format YYYY-MM-DDT00:00:00Z. | string |  |
| _`/custom_insights/-/time_increment`_ | Time Increment | Time window in days by which to aggregate statistics. The sync will be chunked into N day intervals, where N is the number of days you specified. For example, if you set this value to 7, then all statistics will be reported as 7-day aggregates by starting from the start&#x5F;date. If the start and end dates are October 1st and October 30th, then the connector will output 5 records: 01 - 06, 07 - 13, 14 - 20, 21 - 27, and 28 - 30 (3 days only). | integer | `1` |
| `/end_date` | End Date | The date until which you&#x27;d like to capture data, in the format YYYY-MM-DDT00:00:00Z. All data generated between start&#x5F;date and this date will be replicated. Not setting this option will result in always syncing the latest data. | string |  |
| `/fetch_thumbnail_images` | Fetch Thumbnail Images | In each Ad Creative, fetch the thumbnail&#x5F;url and store the result in thumbnail&#x5F;data&#x5F;url | boolean | `false` |
| `/include_deleted` | Include Deleted | Include data from deleted Campaigns, Ads, and AdSets | boolean | `false` |
| `/insights_lookback_window` | Insights Lookback Window | The [attribution window](https://www.facebook.com/business/help/2198119873776795) | integer | `28` |
| `/max_batch_size` | Maximum size of Batched Requests | Maximum batch size used when sending batch requests to Facebook API. Most users do not need to set this field unless they specifically need to tune the connector to address specific issues or use cases. | integer | `50` |
| `/page_size` | Page Size of Requests | Page size used when sending requests to Facebook API to specify number of records per page when response has pagination. Most users do not need to set this field unless they specifically need to tune the connector to address specific issues or use cases. | integer | `25` |
| **`/start_date`** | Start Date | The date from which you&#x27;d like to begin capturing data, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Facebook Marketing account from which collections are captured. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-facebook-marketing:dev
        config:
            access_token: <secret>
            account_id: 000000000000000
            start_date: 2022-03-01T00:00:00Z
            custom_insights:
              - name: my-custom-insight
                 fields: [ad_id, account_currency]
                 breakdowns: [device_platform]
                 action_breakdowns: [action_type]
                 start_date: 2022-03-01T00:00:00Z
    bindings:
      - resource:
          stream: ad_account
          syncMode: incremental
        target: ${PREFIX}/ad_account
      - resource:
          stream: ad_sets
          syncMode: incremental
        target: ${PREFIX}/ad_sets
      - resource:
          stream: ads_insights
          syncMode: incremental
        target: ${PREFIX}/ads_insights
      - resource:
          stream: ads_insights_age_and_gender
          syncMode: incremental
        target: ${PREFIX}/ads_insights_age_and_gender
      - resource:
          stream: ads_insights_country
          syncMode: incremental
        target: ${PREFIX}/ads_insights_country
      - resource:
          stream: ads_insights_region
          syncMode: incremental
        target: ${PREFIX}/ads_insights_region
      - resource:
          stream: ads_insights_dma
          syncMode: incremental
        target: ${PREFIX}/ads_insights_dma
      - resource:
          stream: ads_insights_platform_and_device
          syncMode: incremental
        target: ${PREFIX}/ads_insights_platform_and_device
      - resource:
          stream: ads_insights_action_type
          syncMode: incremental
        target: ${PREFIX}/ads_insights_action_type
      - resource:
          stream: campaigns
          syncMode: incremental
        target: ${PREFIX}/campaigns
      - resource:
          stream: activities
          syncMode: incremental
        target: ${PREFIX}/activities
      - resource:
          stream: ads
          syncMode: incremental
        target: ${PREFIX}/ads
      - resource:
          stream: ad_creatives
          syncMode: full_refresh
        target: ${PREFIX}/ad_creatives
```

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)
