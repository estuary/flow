# Zendesk Chat

This connector captures data from Zendesk into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-zendesk-chat:dev`](https://ghcr.io/estuary/source-zendesk-chat:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Zendesk API:

* [Accounts](https://developer.zendesk.com/rest_api/docs/chat/accounts#show-account)
* [Agents](https://developer.zendesk.com/rest_api/docs/chat/agents#list-agents)
* [Agent Timelines](https://developer.zendesk.com/rest_api/docs/chat/incremental_export#incremental-agent-timeline-export)
* [Chats](https://developer.zendesk.com/rest_api/docs/chat/chats#list-chats)
* [Shortcuts](https://developer.zendesk.com/rest_api/docs/chat/shortcuts#list-shortcuts)
* [Triggers](https://developer.zendesk.com/rest_api/docs/chat/triggers#list-triggers)
* [Bans](https://developer.zendesk.com/rest_api/docs/chat/bans#list-bans)
* [Departments](https://developer.zendesk.com/rest_api/docs/chat/departments#list-departments)
* [Goals](https://developer.zendesk.com/rest_api/docs/chat/goals#list-goals)
* [Skills](https://developer.zendesk.com/rest_api/docs/chat/skills#list-skills)
* [Roles](https://developer.zendesk.com/rest_api/docs/chat/roles#list-roles)
* [Routing Settings](https://developer.zendesk.com/rest_api/docs/chat/routing_settings#show-account-routing-settings)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Zendesk Account with permission to access data from accounts you want to sync.
* An [Access Token](https://developer.zendesk.com/rest_api/docs/chat/auth). We recommend creating a restricted, read-only key specifically for Estuary access to allow you to control which resources Estuary should be able to access.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Zendesk Chat source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/start_date`** | Start Date | The date from which you would like to replicate data for Zendesk Support API, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated. | string | Required |
| **`/subdomain`** | Subdomain | This is your Zendesk subdomain that can be found in your account URL. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource in Zendesk from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-zendesk-chat:dev
        config:
            credentials:
              access_token: <secret>
              credentials: access_token
            start_date: 2022-03-01T00:00:00Z
            subdomain: my_subdomain
    bindings:
      - resource:
          stream: accounts
          syncMode: full_refresh
        target: ${PREFIX}/accounts
```
