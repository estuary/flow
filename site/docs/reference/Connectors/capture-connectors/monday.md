# Monday

This connector captures data from Monday.com into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-monday:dev`](https://ghcr.io/estuary/source-monday:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Monday.com API:

* [Boards](https://developer.monday.com/api-reference/reference/boards)
* [Items](https://developer.monday.com/api-reference/reference/items)
* [Users](https://developer.monday.com/api-reference/reference/users)
* [Teams](https://developer.monday.com/api-reference/reference/teams)
* [Tags](https://developer.monday.com/api-reference/reference/tags)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

Monday.com supports two authentication methods: OAuth and API token. The API token method is the only supported method; however, OAuth will be supported in the future.

### API Token Authentication

Each Monday.com user account has an unique API token that can be accessed in the Monday.com web app by going to the Avatar menu > Developers tab > My access tokens. See [Monday.com's documentation](https://developer.monday.com/apps/docs/choosing-auth#method-3-using-a-users-global-api-token) for more information.

### OAuth Authentication

Coming soon.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Monday source connector.

### Properties

#### Endpoint

The properties in the table below reflect manual authentication using the CLI. In the Flow web app, you'll enter the API token directly.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | Access Token | Monday Access token. | string | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required, `"Private App Credentials"` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-monday:dev
        config:
          credentials:
            credentials_title: Default Monday Credentials
            api_token: <secret>
          advanced:
            limit: 10
    bindings:
      - resource:
          name: boards
        target: ${PREFIX}/boards
      - resource:
          name: items
        target: ${PREFIX}/items
      - resource:
          name: updates
        target: ${PREFIX}/updates
      - resource:
          name: users
        target: ${PREFIX}/users
```
