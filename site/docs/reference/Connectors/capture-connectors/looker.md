# Looker

This connector captures data from [Looker](https://cloud.google.com/looker/docs/reference/looker-api/latest) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-looker:dev`](https://ghcr.io/estuary/source-looker:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Looker API:

* [dashboard_elements](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/Dashboard/dashboard_dashboard_elements)
* [dashboards](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/Dashboard/all_dashboards)
* [folders](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/Folder/all_folders)
* [lookml_model_explores](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/LookmlModel/lookml_model_explore)
* [lookml_models](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/LookmlModel/all_lookml_models)
* [roles](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/Role/all_roles)
* [user_credentials_embed](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/User/all_user_credentials_embeds)
* [user_roles](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/User/user_roles)
* [users](https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/User/all_users)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Looker account.
* A Looker API key, consisting of a client ID and a client secret. See Looker's [documentation](https://cloud.google.com/looker/docs/admin-panel-users-users#api_keys) for instructions on generating these credentials.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Looker source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/client_id`** | Client ID | Looker client ID. | string | Required |
| **`/credentials/client_secret`** | Client Secret | Looker client secret. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Set to `OAuth Credentials`. | string | Required |
| **`/subdomain`** | Subdomain | The subdomain for your Looker account. For example in `https://estuarydemo.cloud.looker.com/folders/home`, `estuarydemo.cloud.looker.com` is the subdomain. | string | Required |

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
        image: ghcr.io/estuary/source-looker:dev
        config:
          credentials:
            client_id: <secret>
            client_secret: <secret>
          subdomain: mylookersubdomain.cloud.looker.com
    bindings:
      - resource:
          name: dashboards
        target: ${PREFIX}/dashboards
```
