# Datadog

This connector captures data from [Datadog](https://docs.datadoghq.com/api/latest) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-datadog:dev`](https://ghcr.io/estuary/source-datadog:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Datadog API:

* [real_user_monitoring](https://docs.datadoghq.com/api/latest/rum)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Datadog account.
* A Datadog API key and Application key with `rum_apps_read` permission scope. See Datadog's [documentation](https://docs.datadoghq.com/account_management/api-app-keys) for instructions on generating these credentials.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Datadog source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | Datadog API Key. | string | Required |
| **`/credentials/application_key`** | Application Key | Datadog Application Key with `rum_apps_read` permissions. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Set to `Private App Credentials`. | string | Required |
| **`/site`** | Site | The cloud region where the Datadog organization is deployed. Datadog's sites can be found [here](https://docs.datadoghq.com/getting_started/site/#access-the-datadog-site). | string | Required |
| `/advanced/start_date` | Start Date | The date that we should attempt to start backfilling from. If not provided, will use [Datadog's 30-day RUM retention period](https://docs.datadoghq.com/real_user_monitoring/rum_without_limits/retention_filters/). | date | Not Required |

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
        image: ghcr.io/estuary/source-datadog:dev
        config:
          credentials:
            credentials_title: Private App Credentials
            access_token: <secret>
            application_key: <secret>
          site: us5.datadoghq.com
    bindings:
      - resource:
          name: real_user_monitoring
        target: ${PREFIX}/real_user_monitoring
```
