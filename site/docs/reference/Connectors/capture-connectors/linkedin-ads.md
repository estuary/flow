# LinkedIn Ads

[IN PROGRESS]

This connector captures data from LinkedIn Ads into Flow collections through the LinkedIn Marketing API.

[`ghcr.io/estuary/linkedin-ads:dev`](https://ghcr.io/estuary/linkedin-ads:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/linkedin-ads/),
but keep in mind that the two versions may be significantly different.

## Supported data resources

The following data resources are supported:

* [Accounts](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts?view=li-lms-2022-08&tabs=http)
* [Account users](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users?view=li-lms-2022-08&tabs=http)
* [Campaign groups](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups?view=li-lms-2022-08&tabs=http)
* [Campaigns](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns?view=li-lms-2022-08&tabs=http)
* [Creatives](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-unversioned&tabs=http)
* [AdDirectSponsoredContents (Video ads)](hhttps://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video?view=li-lms-unversioned&tabs=http)
* [Ad analytics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2022-08&tabs=http) by campaign
* [Ad analytics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2022-08&tabs=http) by creative

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog spec YAML.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and YAML sample below provide configuration details specific to the [CONNECTOR NAME] source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/account_ids` | Account IDs (Optional) | Specify the account IDs separated by a space, to pull the data from. Leave empty, if you want to pull the data from all associated accounts. See the &lt;a href=&quot;https:&#x2F;&#x2F;www.linkedin.com&#x2F;help&#x2F;linkedin&#x2F;answer&#x2F;a424270&#x2F;find-linkedin-ads-account-details?lang=en&quot;&gt;LinkedIn Ads docs&lt;&#x2F;a&gt; for more info. | array | `[]` |
| _`/account_ids/-`_ |  |  | integer |  |
| `/credentials` | Authentication &#x2A; |  | object |  |
| `/credentials/auth_method` |  |  | string |  |
| **`/start_date`** | Start date | UTC date in the format 2020-09-17. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | LinkedIn Ads stream from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample
