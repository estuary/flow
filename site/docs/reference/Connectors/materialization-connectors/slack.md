## Slack

This connector lets you materialize data from Estuary Flow directly into Slack channels.

`ghcr.io/estuary/materialize-slack:dev` provides the latest connector image. For earlier versions, visit the [GitHub Container Registry](https://ghcr.io/estuary/materialize-slack) page.

### Prerequisites

To use this connector, ensure you have the following:

1. An active Slack workspace with appropriate permissions.
2. Properly configured Slack credentials for authentication.

### Configuration

You can set up the Slack destination connector either through the Flow web app or by editing the Flow specification file directly. To learn more about connectors and how to set them up, read our guide on [using connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| /access_token | Access Token | The Slack API access token for authentication. | string | Required |
| /client_id | Client ID | The Slack API client ID for authentication. | string | Required |
| /client_secret | Client Secret | The Slack API client secret for authentication. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| /source | Source | Source data in Flow to be sent to Slack. | string | Required |
| /channel | Channel | The ID of the Slack channel to send messages to. | string | Required |
| /display_name | Display Name | The display name for the sender in Slack. | string | |
| /logo_emoji | Logo Emoji | The emoji to be used. | string |  |

In the example below, the `bindings` section is configured to specify the source, channel, and sender details:

```yaml
bindings:
  - source: ${PREFIX}/source_name
    resource:
      channel: "id: C05A95LJHSL"
      sender_config:
        display_name: Task Monitor
        logo_emoji: ":eyes:
```
