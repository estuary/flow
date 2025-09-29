# Sentry

This connector captures data from Sentry into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sentry:dev`](https://ghcr.io/estuary/source-sentry:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Sentry APIs:

- [Environments](https://docs.sentry.io/api/environments/list-an-organizations-environments/)
- [Teams](https://docs.sentry.io/api/teams/list-an-organizations-teams/)
- [Projects](https://docs.sentry.io/api/projects/list-your-projects/)
- [Issues](https://docs.sentry.io/api/events/list-a-projects-issues/)
- [Releases](https://docs.sentry.io/api/releases/list-an-organizations-releases/)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To set up the Sentry source connector, you'll need a Sentry [auth token](https://docs.sentry.io/api/auth/#auth-tokens) and the [organization](https://docs.sentry.io/product/accounts/membership/) slug.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Sentry source connector.

### Properties

#### Endpoint

| Property                             | Title        | Description                                                                                                                                                                                                                                                          | Type   | Required/Default          |
| ------------------------------------ | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------------- |
| **`/credentials/credentials_title`** | Credentials  | Name of the credentials set                                                                                                                                                                                                                                          | string | Required                  |
| **`/credentials/access_token`**      | Access Token | Sentry auth token.                                                                                                                                                                                                                                                   | string | Required                  |
| **`/organization`**                  | Organization | The slug of the organization.                                                                                                                                                                                                                                        | string | Required                  |
| **`/start_date`**                    | Start Date   | The date from which you'd like to replicate data for Sentry API, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated.                                                                                                          | string | 7 days before the present |
| `/advanced/window_size`              | Window Size  | Date window size for the `issues` backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours. If you have a significant amount of `isssues` data to backfill, smaller window sizes will allow the connector to checkpoint its progress more frequently. | string | P30D                      |

#### Bindings

| Property        | Title         | Description                 | Type   | Required/Default |
| --------------- | ------------- | --------------------------- | ------ | ---------------- |
| **`/name`**     | Data resource | Name of the data resource.  | string | Required         |
| **`/interval`** | Interval      | Interval between data syncs | string | PT5M             |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sentry:dev
        config:
          credentials:
            credentials_title: Private App Credentials
            access_token: <your auth token>
          organization: <your organization>
          start_date: "2025-08-14T00:00:00Z"
          advanced:
            window_size: P10D
    bindings:
      - resource:
          name: Issues
          interval: PT5M
        target: ${PREFIX}/Issues
```
