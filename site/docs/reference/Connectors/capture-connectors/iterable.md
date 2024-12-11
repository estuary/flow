
# Iterable

This connector captures data from Iterable into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-iterable:dev`](https://ghcr.io/estuary/source-iterable:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Iterable APIs:

* [Campaigns](https://api.iterable.com/api/docs#campaigns_campaigns)
* [Campaign Metrics](https://api.iterable.com/api/docs#campaigns_metrics)
* [Channels](https://api.iterable.com/api/docs#channels_channels)
* [Email Bounce](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Click](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Complaint](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Open](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Send](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Send Skip](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Subscribe](https://api.iterable.com/api/docs#export_exportDataJson)
* [Email Unsubscribe](https://api.iterable.com/api/docs#export_exportDataJson)
* [Events](https://api.iterable.com/api/docs#events_User_events)
* [Lists](https://api.iterable.com/api/docs#lists_getLists)
* [List Users](https://api.iterable.com/api/docs#lists_getLists_0)
* [Message Types](https://api.iterable.com/api/docs#messageTypes_messageTypes)
* [Metadata](https://api.iterable.com/api/docs#metadata_list_tables)
* [Templates](https://api.iterable.com/api/docs#templates_getTemplates)
* [Users](https://api.iterable.com/api/docs#export_exportDataJson)
* [PushSend](https://api.iterable.com/api/docs#export_exportDataJson)
* [PushSendSkip](https://api.iterable.com/api/docs#export_exportDataJson)
* [PushOpen](https://api.iterable.com/api/docs#export_exportDataJson)
* [PushUninstall](https://api.iterable.com/api/docs#export_exportDataJson)
* [PushBounce](https://api.iterable.com/api/docs#export_exportDataJson)
* [WebPushSend](https://api.iterable.com/api/docs#export_exportDataJson)
* [WebPushClick](https://api.iterable.com/api/docs#export_exportDataJson)
* [WebPushSendSkip](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppSend](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppOpen](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppClick](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppClose](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppDelete](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppDelivery](https://api.iterable.com/api/docs#export_exportDataJson)
* [InAppSendSkip](https://api.iterable.com/api/docs#export_exportDataJson)
* [InboxSession](https://api.iterable.com/api/docs#export_exportDataJson)
* [InboxMessageImpression](https://api.iterable.com/api/docs#export_exportDataJson)
* [SmsSend](https://api.iterable.com/api/docs#export_exportDataJson)
* [SmsBounce](https://api.iterable.com/api/docs#export_exportDataJson)
* [SmsClick](https://api.iterable.com/api/docs#export_exportDataJson)
* [SmsReceived](https://api.iterable.com/api/docs#export_exportDataJson)
* [SmsSendSkip](https://api.iterable.com/api/docs#export_exportDataJson)
* [SmsUsageInfo](https://api.iterable.com/api/docs#export_exportDataJson)
* [Purchase](https://api.iterable.com/api/docs#export_exportDataJson)
* [CustomEvent](https://api.iterable.com/api/docs#export_exportDataJson)
* [HostedUnsubscribeClick](https://api.iterable.com/api/docs#export_exportDataJson)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Iterable source connector, you'll need the Iterable [`Server-side` API Key with `standard` permissions](https://support.iterable.com/hc/en-us/articles/360043464871-API-Keys-).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Iterable source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/apikey` | API Key | The value of the Iterable API Key generated. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Iterable project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-iterable:dev
        config:
          apikey: <secret>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: purchase
          syncMode: full_refresh
        target: ${PREFIX}/purchase
      {...}
```
