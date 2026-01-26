# Iterable

This connector captures data from Iterable into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-iterable-native:dev`](https://ghcr.io/estuary/source-iterable-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [campaign_metrics](https://api.iterable.com/api/docs#campaigns_metrics) | Incremental |
| [campaigns](https://api.iterable.com/api/docs#campaigns_campaigns) | Incremental |
| [channels](https://api.iterable.com/api/docs#channels_channels) | Full Refresh |
| [events](https://api.iterable.com/api/docs#export_startExport) | Incremental |
| [list_users](https://api.iterable.com/api/docs#lists_getUsers) | Full Refresh |
| [lists](https://api.iterable.com/api/docs#lists_getLists) | Full Refresh |
| [message_types](https://api.iterable.com/api/docs#messageTypes_messageTypes) | Full Refresh |
| [metadata_tables](https://api.iterable.com/api/docs#metadata_list_tables) | Full Refresh |
| [templates](https://api.iterable.com/api/docs#templates_getTemplates) | Full Refresh |
| [users](https://api.iterable.com/api/docs#export_startExport) | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

To set up the Iterable source connector, you'll need an Iterable [server-side API key](https://support.iterable.com/hc/en-us/articles/360043464871-API-Keys-) with standard permissions.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Iterable source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | The Iterable API key. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `Private App Credentials`. | string | Required |
| **`/project_type`** | Project Type | The type of Iterable [project](#project-types-and-user-identification), which determines how users are uniquely identified. One of `Email-based`, `UserID-based`, or `Hybrid`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string | |
| `/advanced/window_size` | Window Size | Date window size for export jobs in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours. If you have a significant amount of `events` or `users` data to backfill, smaller window sizes will allow the connector to checkpoint its progress more frequently. | string | PT12H |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-iterable-native:dev
        config:
          advanced:
            window_size: PT2H
          credentials:
            credentials_title: Private App Credentials
            access_token: <secret>
          project_type: Email-based
          start_date: 2024-01-01T00:00:00Z
    bindings:
      - resource:
          name: campaigns
        target: ${PREFIX}/campaigns
      - resource:
          name: events
        target: ${PREFIX}/events
      {...}
```

## Project Types and User Identification

The configured project type determines how the `users` stream identifies records:

| Project Type | Primary Key |
|--------------|-------------|
| Email-based | `email` |
| UserID-based | `itblUserId` |
| Hybrid | `itblUserId` |

See [Iterable's documentation](https://support.iterable.com/hc/en-us/articles/9216719179796-Project-Types-and-Unique-Identifiers) for more information.

## Events Synthetic ID

The `events` stream lacks a natural unique identifier from Iterable. The connector computes a synthetic ID `_estuary_id` by hashing `createdAt`, `email`, `itblUserId`, `campaignId`, `eventName`, and `eventType` to deduplicate events.
