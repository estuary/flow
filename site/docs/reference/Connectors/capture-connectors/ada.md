# Ada

This connector captures data from [Ada](https://www.ada.cx/) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-ada:dev`](https://ghcr.io/estuary/source-ada:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Ada API:


* [articles](https://docs.ada.cx/reference/knowledge/articles/list)
* [conversations](https://docs.ada.cx/data-export-conversation-object/get-conversations)
* [messages](https://docs.ada.cx/data-export-message-object/get-messages)
* [sources](https://docs.ada.cx/reference/knowledge/articles/list)
* [tags](https://docs.ada.cx/reference/knowledge/tags/list)
* [platform_integrations](https://docs.ada.cx/reference/integrations/get-platform-integrations)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* An Ada account.
* An Ada API key. See Ada's [documentation](https://docs.ada.cx/reference/introduction/authentication#generate-an-ada-api-key) for instructions on generating an API key.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Ada source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/bot_handle`** | Bot Handle | Your Ada bot handle. This can be found in the URI of your bot's dashboard. ex: In `BOT_HANDLE.ada.support`, `BOT_HANDLE` is your Ada bot handle. | string | Required |
| **`/credentials/access_token`** | API Key | Your Ada API key. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Set to `Private App Credentials`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 12 months before the present date. Note: due to Ada's data retention limits, only the past 12 months of data can be captured. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-ada:dev
        config:
          bot_handle: my_bot_handle
          credentials:
            access_token: <secret>
          start_date: 2025-12-19T00:00:00Z
    bindings:
      - resource:
          name: conversations
        target: ${PREFIX}/conversations
```
