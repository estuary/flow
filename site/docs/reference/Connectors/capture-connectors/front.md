# Front

This connector captures data from Front into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-front:dev`](https://ghcr.io/estuary/source-front:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Front API:

* [channels](https://dev.frontapp.com/reference/list-channels)
* [contacts](https://dev.frontapp.com/reference/list-contacts)
* [conversations](https://dev.frontapp.com/reference/search-conversations)
* [events](https://dev.frontapp.com/reference/list-events)
* [inboxes](https://dev.frontapp.com/reference/list-inboxes)
* [tags](https://dev.frontapp.com/reference/list-tags)
* [teammates](https://dev.frontapp.com/reference/list-teammates)
* [teams](https://dev.frontapp.com/reference/list-teams)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Front [API token](https://dev.frontapp.com/docs/create-and-revoke-api-tokens) with the appropriate scope(s).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Front source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | Your Front API token. | string | Required |
| `/start_date` | Replication Start Date | UTC date and time in the format "YYYY-MM-DDTHH:MM:SSZ". Data prior to this date will not be replicated. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Resource in Front from which collections are captured. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-front:dev
        config:
          credentials:
            access_token: <secret>
          start_date: "2024-11-15T00:00:00Z"
    bindings:
      - resource:
          name: channels
        target: ${PREFIX}/channels
      - resource:
          name: contacts
        target: ${PREFIX}/contacts
      - resource:
          name: conversations
        target: ${PREFIX}/conversations
      - resource:
          name: events
        target: ${PREFIX}/events
      - resource:
          name: inboxes
        target: ${PREFIX}/inboxes
      - resource:
          name: tags
        target: ${PREFIX}/tags
      - resource:
          name: teammates
        target: ${PREFIX}/teammates
      - resource:
          name: teams
        target: ${PREFIX}/teams
```
