
# Google Ads

This connector captures data from [resources](https://developers.google.com/google-ads/api/fields/v11/overview) in one or more Google Ads accounts into Flow collections via the Google Ads API.

[`ghcr.io/estuary/source-google-ads:dev`](https://ghcr.io/estuary/source-google-ads:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported.
Resources ending in `_report` represent legacy resources from the [Google Adwords API](https://developers.google.com/google-ads/api/docs/migration).

* [ad_group_ads](https://developers.google.com/google-ads/api/fields/latest/ad_group_ad)
* [ad_group_ad_label](https://developers.google.com/google-ads/api/fields/latest/ad_group_ad_label)
* [ad_groups](https://developers.google.com/google-ads/api/fields/latest/ad_group)
* [ad_group_label](https://developers.google.com/google-ads/api/fields/latest/ad_group_label)
* [campaigns](https://developers.google.com/google-ads/api/fields/v9/campaign)
* [campaign_labels](https://developers.google.com/google-ads/api/fields/latest/campaign_label)
* [click_view](https://developers.google.com/google-ads/api/reference/rpc/latest/ClickView)
* [customer](https://developers.google.com/google-ads/api/fields/latest/customer)
* [geographic_view](https://developers.google.com/google-ads/api/fields/latest/geographic_view)
* [keyword_view](https://developers.google.com/google-ads/api/fields/latest/keyword_view)
* [user_location_view](https://developers.google.com/google-ads/api/fields/latest/user_location_view)
* [account_performance_report](https://developers.google.com/google-ads/api/docs/migration/mapping#account_performance)
* [ad_performance_report](https://developers.google.com/google-ads/api/docs/migration/mapping#ad_performance)
* [display_keyword_performance_report](https://developers.google.com/google-ads/api/docs/migration/mapping#display_keyword_performance)
* [display_topics_performance_report](https://developers.google.com/google-ads/api/docs/migration/mapping#display_topics_performance)
* [shopping_performance_report](https://developers.google.com/google-ads/api/docs/migration/mapping#shopping_performance)

By default, each resource is mapped to a Flow collection through a separate binding.

You may also generate custom resources using [GAQL queries](#custom-queries).

## Prerequisites

There are two ways to authenticate with Google when capturing data into Flow: using OAuth2, and manually, using tokens and secret credentials.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the manual method is the only supported method using the command line.

### Customer Id & Login Customer Id

The `Login Customer Id` setting refers to your MCC Google Ads account Id.
One can easily find this number by accessing their Google Ads Dashboard and look to the far right corner of their screen.

Example:

![Screenshot from 2024-02-19 05-10-29](https://github.com/estuary/flow/assets/14100959/f20aeeef-eeac-432f-b547-11477e31661d)

In the above example, my `login_customer_id` would be 1234567890.

The `Customer Id` setting refers to your Client Accounts under a MCC account.
One can easily find this number by accessing their Google Ads Dashboard and look to the far left corner of their screen,
after selecting a client account.

Example:

![Screenshot from 2024-02-19 05-10-16](https://github.com/estuary/flow/assets/14100959/4f171fa7-9c82-4f24-8a1d-8aacd382fb28)

In the above example, my `customer_id` would be 9876543210.

#### Multiple Customer Ids

This Source allows for multiple Customer Ids to be selected.
To allow this, simply add your `customer_id` followed by a comma.

Example:

Customer1 = 1234567890
Customer2 = 9876543210

customer_id = 1234567890,9876543210

### Using OAuth2 to authenticate with Google in the Flow web app

* One or more Google Ads accounts.

   * Note each account's [customer ID](https://support.google.com/google-ads/answer/1704344)

* A Google Account that has [access](https://support.google.com/google-ads/answer/6372672?hl=en) to the Google Ads account(s).

  * This account may be a [**manager account**](https://ads.google.com/home/tools/manager-accounts/).
  If so, ensure that it is [linked to each Google Ads account](https://support.google.com/google-ads/answer/7459601) and make note of its [customer ID](https://support.google.com/google-ads/answer/29198?hl=en).

### Configuring the connector specification manually

* One or more Google Ads accounts.

   * Note each account's [customer ID](https://support.google.com/google-ads/answer/1704344?hl=en)

* A Google Ads [**manager account**](https://ads.google.com/home/tools/manager-accounts/) that has been [linked to each Google Ads account](https://support.google.com/google-ads/answer/7459601)

* A Google Ads [developer token](https://developers.google.com/google-ads/api/docs/first-call/dev-token?hl=en). Your Google Ads manager account must be configured prior to applying for a developer token.

:::caution
Developer token applications are independently reviewed by Google and may take one or more days to be approved.
Be sure to carefully review Google's requirements before submitting an application.
:::

* A [refresh token](https://developers.google.com/google-ads/api/docs/first-call/refresh-token?hl=en), which fetches a new developer tokens for you as the previous token expires.

* A generated [Client ID and Client Secret](https://developers.google.com/google-ads/api/docs/oauth/cloud-project#create_a_client_id_and_client_secret), used for authentication.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Google Ads source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-google-in-the-flow-web-app),
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/conversion_window_days` | Conversion Window (Optional) | A conversion window is the period of time after an ad interaction (such as an ad click or video view) during which a conversion, such as a purchase, is recorded in Google Ads. For more information, see [Google&#x27;s docs](https://support.google.com/google-ads/answer/3123169?hl=en). | integer | `14` |
| **`/credentials`** | Google Credentials |  | object | Required |
| **`/credentials/client_id`** | Client ID | The Client ID of your Google Ads developer application. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The Client Secret of your Google Ads developer application. | string | Required |
| **`/credentials/developer_token`** | Developer Token | Developer token granted by Google to use their APIs. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | The token for obtaining a new access token. | string | Required |
| `/custom_queries` | Custom GAQL Queries (Optional) |  | array |  |
| _`/custom_queries/-/query`_ | Custom Query | A custom defined GAQL query for building the report. Should not contain segments.date expression. See Google&#x27;s [query builder](https://developers.google.com/google-ads/api/fields/v11/overview_query_builder) for more information. | string |  |
| _`/custom_queries/-/table_name`_ | Destination Table Name | The table name in your destination database for chosen query. | string |  |
| **`/customer_id`** | Customer ID(s) | Comma separated list of (client) customer IDs. Each customer ID must be specified as a 10-digit number without dashes. More instruction on how to find this value in our docs.  Metrics streams like AdGroupAdReport cannot be requested for a manager account. | string | Required |
| `/end_date` | End Date (Optional) | UTC date in the format 2017-01-25. Any data after this date will not be replicated. | string |  |
| `/login_customer_id` | Login Customer ID for Managed Accounts (Optional) | If your access to the customer account is through a manager account, this field is required and must be set to the customer ID of the manager account (10-digit number without dashes). | string |  |
| **`/start_date`** | Start Date | UTC date in the format 2017-01-25. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Google Ad resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-ads:dev
        config:
          conversion_window_days: 7
          credentials:
            client_id: {secret_client_ID}
            client_secret: {secret_secret}
            developer_token: {access_token}
            refresh_token: {refresh_token}
          customer_id: 0123456789, 1234567890
          login_customer_id: 0987654321
          end_date: 2022-01-01
          start_date: 2020-01-01
          custom_queries:
            - query:
                SELECT
                  campaign.id,
                  campaign.name,
                  campaign.status
                FROM campaign
                ORDER BY campaign.id
              table_name: campaigns_custom
    bindings:
      - resource:
          stream: campaign
          syncMode: incremental
        target: ${PREFIX}/campaign
      {...}
```

## Custom queries

You can create custom resources using Google Analytics Query Language (GAQL) queries.
Each generated resource will be mapped to a Flow collection.
For help generating a valid query, see [Google's query builder documentation](https://developers.google.com/google-ads/api/fields/v11/overview_query_builder).

If a query fails to validate against a given Google Ads account, it will be skipped.

## Stream Limitations

### ClickView

Due to Google Ads API limitations, ClickView stream queries are executed with a time range limited to one day.
Also, data can only be requested for periods 90 days before the time of the request.

In pratical terms, this means that you can only search ClickView data limited to 3 months ago, anything before this is not returned.

For more information, check [Google's Ads API documentation](https://developers.google.com/google-ads/api/fields/v15/click_view)
