
# Amazon Ads

This connector captures data from Amazon Ads into Flow collections via the [Amazon Ads API](https://advertising.amazon.com/API/docs/en-us).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-amazon-ads:dev`](https://ghcr.io/estuary/source-amazon-ads:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.


## Supported data resources

The following data resources are supported:

* [Profiles](https://advertising.amazon.com/API/docs/en-us/reference/2/profiles#/Profiles)
* [Sponsored brands ad groups](https://advertising.amazon.com/API/docs/en-us/sponsored-brands/3-0/openapi#/Ad%20groups)
* [Sponsored brands campaigns](https://advertising.amazon.com/API/docs/en-us/sponsored-brands/3-0/openapi#/Campaigns)
* [Sponsored brands keywords](https://advertising.amazon.com/API/docs/en-us/sponsored-brands/3-0/openapi#/Keywords)
* [Sponsored brands report stream](https://advertising.amazon.com/API/docs/en-us/sponsored-brands/3-0/openapi#/Reports)
* [Sponsored brands video report stream](https://advertising.amazon.com/API/docs/en-us/sponsored-brands/3-0/openapi#/Reports)
* [Sponsored display ad groups](https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Ad%20groups)
* [Sponsored display ad campaigns](https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Campaigns)
* [Sponsored display product ads](https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Product%20ads)
* [ Sponsored display report stream](https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Reports)
* [Sponsored display targetings](https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Targeting)
* [Sponsored product ad groups](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Ad%20groups)
* [Sponsored product ads](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Product%20ads)
* [Sponsored product campaigns](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Campaigns)
* [Sponsored product keywords](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Keywords)
* [Sponsored product negative keywords](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Negative%20keywords)
* [Sponsored product targetings](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Product%20targeting)
* [Sponsored product report stream](https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Reports)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

This connector uses OAuth2 to authenticate with Amazon. You can do this in the Flow web app, or configure manually if you're using the flowctl CLI.

### Using OAuth2 to authenticate with Amazon in the Flow web app

You'll need an Amazon user account with [access](https://advertising.amazon.com/help?ref_=a20m_us_blg#GDQVHVQMY9F88PCA) to the [Amazon Ads account](https://advertising.amazon.com/register) from which you wish to capture data.

You'll use these credentials to sign in.

### Authenticating manually using the CLI

When you configure this connector manually, you provide the same credentials that OAuth2 would automatically
fetch if you used the web app. These are:

* **Client ID**
* **Client secret**
* **Refresh token**

To obtain these credentials:

1. Complete the [Amazon Ads API onboarding process](https://advertising.amazon.com/API/docs/en-us/onboarding/overview).

2. [Retrieve your client ID and client secret](https://advertising.amazon.com/API/docs/en-us/getting-started/retrieve-access-token#retrieve-your-client-id-and-client-secret).

3. [Retrieve a refresh token](https://advertising.amazon.com/API/docs/en-us/getting-started/retrieve-access-token#call-the-authorization-url-to-request-access-and-refresh-tokens).

## Selecting data region and profiles

When you [configure the endpoint](#endpoint) for this connector, you must choose an Amazon region from which to capture data.
Optionally, you may also select profiles from which to capture data.

The **region** must be one of:

* NA (North America)
* EU (European Union)
* FE (Far East)

These represent the three URL endpoints provided by Amazon through which you can access the marketing API.
Each region encompasses multiple Amazon marketplaces, which are broken down by country.
See the [Amazon docs](https://advertising.amazon.com/API/docs/en-us/info/api-overview#api-endpoints) for details.

If you run your Amazon ads in multiple marketplaces, you may have separate [profiles](https://advertising.amazon.com/API/docs/en-us/concepts/authorization/profiles) for each.
If this is the case, you can specify the profiles from which you wish to capture data
by supplying their [profile IDs](https://advertising.amazon.com/API/docs/en-us/concepts/authorization/profiles#retrieving-profiles-2).
Be sure to specify only profiles that correspond to marketplaces within the region you chose.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Amazon Ads source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-amazon-in-the-flow-web-app),
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** |  |  | object | Required |
| `/credentials/auth_type` | Auth Type | Set to `oauth2.0` for manual integration (in this method, you're re-creating the same credentials of the OAuth user interface, but doing so manually) | string |  |
| **`/credentials/client_id`** | Client ID | The client ID of your Amazon Ads developer application. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The client secret of your Amazon Ads developer application. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | Amazon Ads refresh token. | string | Required |
| `/profiles` | Profile IDs (Optional) | [Profile IDs](#selecting-data-region-and-profiles) you want to fetch data for.  | array |  |
| `/region` | Region &#x2A; | [Region](#selecting-data-region-and-profiles) to pull data from (EU&#x2F;NA&#x2F;FE). | string | `"NA"` |
| `/report_generation_max_retries` | Report Generation Maximum Retries &#x2A; | Maximum retries the connector will attempt for fetching report data. | integer | `5` |
| `/report_wait_timeout` | Report Wait Timeout &#x2A; | Timeout duration in minutes for reports. | integer | `60` |
| `/start_date` | Start Date (Optional) | The start date for collecting reports, in YYYY-MM-DD format. This should not be more than 60 days in the past. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Amazon Ads resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-amazon-ads:dev
          config:
            credentials:
              auth_type: oauth2.0
              client_id: amzn1.application-oa2-client.XXXXXXXXX
              client_secret: <secret>
              refresh_token: Atzr|XXXXXXXXXXXX
            region: NA
            report_generation_max_retries: 5
            report_wait_timeout: 60
            start_date: 2022-03-01

      bindings:
        - resource:
            stream: profiles
            syncMode: full_refresh
          target: ${PREFIX}/profiles
       {}
```
