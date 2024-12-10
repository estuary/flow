# Sentry

This connector captures data from Sentry into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sentry:dev`](https://ghcr.io/estuary/source-sentry:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the Sentry APIs:

* [Events](https://docs.sentry.io/api/events/list-a-projects-events/)
* [Issues](https://docs.sentry.io/api/events/list-a-projects-issues/)
* [Projects](https://docs.sentry.io/api/projects/list-your-projects/)
* [Releases](https://docs.sentry.io/api/releases/list-an-organizations-releases/)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* To set up the Sentry source connector, you'll need the Sentry [project name](https://docs.sentry.io/product/projects/), [authentication token](https://docs.sentry.io/api/auth/#auth-tokens), and [organization](https://docs.sentry.io/product/accounts/membership/).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Sentry source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/auth_token` | Auth Token | Auth Token generated in Sentry | string | Required |
| `/organization` | Organization | The slug of the organization the groups belong to. | string | Required |
| `/project` | Project | The name (slug) of the Project you want to sync. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Sentry project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sentry:dev
        config:
          auth_token: <secret>
          organization: <your organization>
          project: <your project>
    bindings:
      - resource:
          stream: events
          syncMode: full_refresh
        target: ${PREFIX}/events
      {...}
```