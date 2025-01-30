# Slack

This connector lets you materialize data from Estuary Flow directly into Slack channels.

`ghcr.io/estuary/materialize-slack:dev` provides the latest connector image. For earlier versions, visit the [GitHub Container Registry](https://ghcr.io/estuary/materialize-slack) page.

### Prerequisites

To use this connector, ensure you have the following:

1. An active Slack workspace with appropriate permissions.
2. Slack credentials and access token for authentication.
3. At least one Flow collection.

### Configuration

The Slack connector is available for use in the Flow web application. To learn more about connectors and how to set them up, read our guide on [using connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/access_token` | Access Token | The Slack API access token for authentication. | string | Required |
| `/client_id` | Client ID | Client ID for authentication. | string | Required |
| `/client_secret` | Client Secret | The Slack API client secret. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/source` | Source | Source data in Flow to be sent to Slack. | string | Required |
| `/channel` | Channel | The ID of the Slack channel to send messages to. | string | Required |
| `/display_name` | Display Name | The display name for the sender in Slack. | string | |
| `/logo_emoji` | Logo Emoji | The emoji to be used. | string |  |

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-slack:dev
        config:
          credentials:
            auth_type: OAuth
            access_token: {secret}
            client_id: {your_client_id}
            client_secret: {secret}
    bindings:
      - source: ${PREFIX}/source_name
        resource:
          channel: "id: C05A95LJHSL"
          sender_config:
            display_name: Task Monitor
            logo_emoji: ":eyes:"
```
