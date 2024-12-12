
# LinkedIn Ads

This connector captures data from LinkedIn Ads into Flow collections through the LinkedIn Marketing API.

[`ghcr.io/estuary/source-linkedin-ads:dev`](https://ghcr.io/estuary/source-linkedin-ads:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported:

* [Accounts](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts?view=li-lms-2022-08&tabs=http)
* [Account users](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users?view=li-lms-2022-08&tabs=http)
* [Campaign groups](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups?view=li-lms-2022-08&tabs=http)
* [Campaigns](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns?view=li-lms-2022-08&tabs=http)
* [Creatives](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-unversioned&tabs=http)
* [AdDirectSponsoredContents (Video ads)](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video?view=li-lms-unversioned&tabs=http)
* [Ad analytics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2022-08&tabs=http) by campaign
* [Ad analytics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2022-08&tabs=http) by creative

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

There are two ways to authenticate with LinkedIn when capturing data into Flow: using OAuth2, and manually, by creating a developer application.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the developer application method is the only supported method using the command line.

### Using OAuth2 to authenticate with LinkedIn in the Flow web app

* One or more LinkedIn [Ad Accounts](https://www.linkedin.com/help/lms/answer/a426102/create-an-ad-account?lang=en) with active campaigns.

* A LinkedIn user with [access to the Ad Accounts](https://www.linkedin.com/help/lms/answer/a425731/user-roles-and-permissions-in-campaign-manager?lang=en) from which you want to capture data.

### Configuring the connector specification manually

To configure without using OAuth, you'll need to create an application using the LinkedIn Marketing API,
and generate its access token.

#### Setup

1. Create a marketing application on [LinkedIn Developers](https://www.linkedin.com/developers/apps/new).
2. [Apply to the LinkedIn Developer Program](https://docs.microsoft.com/en-us/linkedin/marketing/getting-access?view=li-lms-2022-08#how-to-apply-to-the-marketing-developer-platform).
3. [Generate your access token](https://docs.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow?context=linkedin%2Fcontext&view=li-lms-2022-08&tabs=HTTPS).

:::caution
LinkedIn access tokens expire in 60 days.
You must manually update your capture configuration to continue to capture data from LinkedIn.
:::

## Configuration

You configure connectors either in the Flow web app, or by directly editing the capture specification.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the LinkedIn Ads source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-linkedin-in-the-flow-web-app),
so some of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/account_ids` | Account IDs (Optional) | A space-separated list of the [account IDs](https://www.linkedin.com/help/linkedin/answer/a424270/find-linkedin-ads-account-details?lang=en) from which to capture data. Leave empty if you want to capture data from all linked accounts. | array | `[]` |
| `/credentials` | Authentication |  | object |  |
| `/credentials/auth_method` | Authentication method | Set to `access_token` to authenticate manually. | string |  |
| `/credentials/access_token` | Access token | Access token generated from your LinkedIn Developers app. | string | |
| **`/start_date`** | Start date | UTC date in the format 2020-09-17. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | LinkedIn Ads stream from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-linkedin-ads:dev
        config:
          account_ids:
            - 000000000
            - 111111111
          credentials:
            auth_method: access_token
            access_token: {secret}
          start_date: 2022-01-01
    bindings:
      - resource:
          stream: campaigns
          syncMode: incremental
        target: ${PREFIX}/campaign
      {...}
```
