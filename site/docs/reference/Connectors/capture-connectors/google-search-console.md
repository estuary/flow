
# Google Search Console

This connector captures data from Google Search Console into Flow collections via the [Google Search Console API](https://developers.google.com/webmaster-tools/v1/api_reference_index).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-google-search-console:dev`](https://ghcr.io/estuary/source-google-search-console:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

* [Search analytics: all fields](https://developers.google.com/webmaster-tools/v1/searchanalytics)
    * This resource contains all data in for your search analytics, and can be large. The following five collections come from queries applied to this dataset.
* Search analytics by country
* Search analytics by date
* Search analytics by device
* Search analytics by page
* Search analytics by query
* [Sitemaps](https://developers.google.com/webmaster-tools/v1/sitemaps)
* [Sites](https://developers.google.com/webmaster-tools/v1/sites)

By default, each resource is mapped to a Flow collection through a separate binding.

### Custom reports

In addition to the resources listed above, you can add custom reports created with the [Google Analytics Search Console integration](https://support.google.com/analytics/topic/1308589?hl=en&ref_topic=3125765).
You add these to the [endpoint configuration](#endpoint) in the format `{"name": "<report-name>", "dimensions": ["<dimension-name>", ...]}`.
Each report is mapped to an additional Flow collection.

:::caution
Custom reports involve an integration with Google Universal Analytics, which Google will deprecate in July 2023.
:::

## Prerequisites

There are two ways to authenticate with Google when capturing data from Google Search Console: using OAuth2, and manually, by generating a service account key.
Their prerequisites differ.

OAuth2 is recommended for simplicity in the Flow web app;
the service account key method is the only supported method using the command line.

### Using OAuth2 to authenticate with Google in the Flow web app

You'll need:

* Google credentials with [Owner access](https://support.google.com/webmasters/answer/7687615?hl=en) on the Google Search Console property. This can be a user account or a [service account](https://cloud.google.com/iam/docs/service-accounts).

You'll use these credentials to log in to Google in the Flow web app.

### Authenticating manually with a service account key

You'll need:

* A Google service account with:
  * A JSON key generated.
  * Access to the Google Search Console view through the API.

Follow the steps below to meet these prerequisites:

1. Create a [service account and generate a JSON key](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount)
You'll copy the contents of the downloaded key file into the Service Account Credentials parameter when you configure the connector.

2. [Set up domain-wide delegation for the service account](https://developers.google.com/workspace/guides/create-credentials#optional_set_up_domain-wide_delegation_for_a_service_account).
   1. During this process, grant the `https://www.googleapis.com/auth/webmasters.readonly` OAuth scope.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Google Search Console source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-google-in-the-flow-web-app),
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication |  | object | Required |
| **`/credentials/auth_type`** | Authentication Type | Set to `Service` for manual authentication | string | Required |
| **`/credentials/service_account_info`** | Service Account JSON Key | The JSON key of the service account to use for authorization. | Required
| **`/credentials/email`** | Admin Email | The email of your [Google Workspace administrator](https://support.google.com/a/answer/182076?hl=en). This is likely the account used during setup.  |
| `/custom_reports` | Custom Reports (Optional) | A JSON array describing the [custom reports](#custom-reports) you want to sync from Google Search Console.  | string |  |
| `/end_date` | End Date | UTC date in the format 2017-01-25. Any data after this date will not be replicated. Must be greater or equal to the start date field. | string |  |
| **`/site_urls`** | Website URL | The [URLs of the website properties](https://support.google.com/webmasters/answer/34592?hl=en) attached to your GSC account: <ul><li>domain:example.com</li><li> https:<span></span>//example.com/</li></ul>  This connector supports both URL-prefix and domain property URLs.  | array | Required |
| **`/start_date`** | Start Date | UTC date in the format 2017-01-25. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Google Search Consol resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-search-console:dev
          config:
            credentials:
              auth_type: Service
              service_account_info: <secret>
              email: admin@yourdomain.com
            site_urls: https://yourdomain.com
            start_date: 2022-03-01

      bindings:
        - resource:
            stream: sites
            syncMode: full_refresh
          target: ${PREFIX}/sites
       {}
```
