
# Asana

This connector captures data from [Asana's REST API](https://developers.asana.com/reference/rest-api-reference).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-asana:dev`](https://ghcr.io/estuary/source-asana:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Asana APIs:

### Default Streams

* [Attachments](https://developers.asana.com/reference/attachments)
* [Attachments (Compact)](https://developers.asana.com/reference/attachments#attachmentcompact)
* [Custom Fields](https://developers.asana.com/reference/custom-fields)
* [Events](https://developers.asana.com/reference/events)
* [Portfolios](https://developers.asana.com/reference/portfolios)
* [Portfolios (Compact)](https://developers.asana.com/reference/portfolios#portfoliocompact)
* [Portfolios Memberships](https://developers.asana.com/reference/portfolio-memberships)
* [Projects](https://developers.asana.com/reference/projects)
* [Sections](https://developers.asana.com/reference/sections)
* [Sections (Compact)](https://developers.asana.com/reference/sections#sectioncompact)
* [Stories](https://developers.asana.com/reference/stories)
* [Stories (Compact)](https://developers.asana.com/reference/stories#storycompact)
* [Tags](https://developers.asana.com/reference/tags)
* [Tasks](https://developers.asana.com/reference/tasks)
* [Team Memberships](https://developers.asana.com/reference/team-memberships)
* [Teams](https://developers.asana.com/reference/teams)
* [Users](https://developers.asana.com/reference/users)
* [Workspaces](https://developers.asana.com/reference/workspaces)

### Streams Available for Enterprise+ Organizations

* [Organization Exports](https://developers.asana.com/reference/organization-exports) (available for service accounts)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

You will need an Asana account.

You can authenticate your account with Estuary either via OAuth or using an Asana [personal access token](https://developers.asana.com/docs/personal-access-token).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Asana source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials` | Credentials |  | object | Required |
| `/credentials/option_title` | Credentials Title | Denotes the authentication type. Can either be `OAuth Credentials` or `PAT Credentials`. | string |  |
| `/credentials/client_id` | Client ID | The client ID for Asana OAuth. | string | Required when using the `OAuth Credentials` option |
| `/credentials/client_secret` | Client Secret | The client secret for Asana OAuth. | string | Required when using the `OAuth Credentials` option |
| `/credentials/refresh_token` | Refresh Token | The refresh token for Asana OAuth. | string | Required when using the `OAuth Credentials` option |
| `/credentials/personal_access_token` | Personal Access Token | The access token to authenticate with the Asana API. | string | Required when using the `PAT Credentials` option |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Asana resource from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-asana:dev
        config:
          credentials:
            option_title: PAT Credentials
            personal_access_token: <secret>
    bindings:
      - resource:
          stream: attachments
          syncMode: full_refresh
        target: ${PREFIX}/attachments
      {...}
```
