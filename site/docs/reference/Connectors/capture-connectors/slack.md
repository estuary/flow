# Slack

This connector captures data from Slack into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-slack:dev`](https://ghcr.io/estuary/source-slack:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Slack APIs:

* [Channels (Conversations)](https://api.slack.com/methods/conversations.list)
* [Channel Members (Conversation Members)](https://api.slack.com/methods/conversations.members)
* [Messages (Conversation History)](https://api.slack.com/methods/conversations.history)
* [Users](https://api.slack.com/methods/users.list)
* [Threads (Conversation Replies)](https://api.slack.com/methods/conversations.replies)
* [User Groups](https://api.slack.com/methods/usergroups.list)
* [Files](https://api.slack.com/methods/files.list)
* [Remote Files](https://api.slack.com/methods/files.remote.list)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* Slack workspace URL or API token for authentication.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Slack source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/join_channels` | Join Channels | Whether to join all channels | boolean | `true` |
| `/lookback_window` | Threads Lookback window (Days) | How far into the past to look for messages in threads. | integer | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Slack project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-slack:dev
        config:
          credentials:
            auth_type: OAuth
            access_token: {secret}
            client_id: {your_client_id}
            client_secret: {secret}
          join_channels: true
          lookback_window: 7
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: channel_members
          syncMode: full_refresh
        target: ${PREFIX}/channel_members
      {...}
```