---
description: Use Estuary's Sentry connector to sync environments, teams, projects, issues, releases, and custom Explore queries, using a Sentry access token.
---

# Sentry

This connector captures data from Sentry into Estuary collections.

## Supported data resources

The following data resources are supported through the Sentry APIs:

- [Environments](https://docs.sentry.io/api/environments/list-an-organizations-environments/)
- [Teams](https://docs.sentry.io/api/teams/list-an-organizations-teams/)
- [Projects](https://docs.sentry.io/api/projects/list-your-projects/)
- [Issues](https://docs.sentry.io/api/events/list-a-projects-issues/)
- [Releases](https://docs.sentry.io/api/releases/list-an-organizations-releases/)

By default, each resource is mapped to an Estuary collection through a separate binding.

In addition to these built-in resources, you can define your own incremental streams backed by Sentry's [Explore events API](https://docs.sentry.io/api/explore/query-explore-events-in-table-format/). See [Custom Explore queries](#custom-explore-queries) below.

## Prerequisites

To set up the Sentry source connector, you'll need a Sentry [auth token](https://docs.sentry.io/api/auth/#auth-tokens) and the [organization](https://docs.sentry.io/product/accounts/membership/) slug.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Sentry source connector.

### Properties

#### Endpoint

| Property                             | Title        | Description                                                                                                                                                                                                                                                          | Type   | Required/Default          |
| ------------------------------------ | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------------- |
| **`/credentials/credentials_title`** | Credentials  | Name of the credentials set                                                                                                                                                                                                                                          | string | Required                  |
| **`/credentials/access_token`**      | Access Token | Sentry auth token.                                                                                                                                                                                                                                                   | string | Required                  |
| **`/organization`**                  | Organization | The slug of the organization.                                                                                                                                                                                                                                        | string | Required                  |
| **`/start_date`**                    | Start Date   | The date from which you'd like to replicate data for Sentry API, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated.                                                                                                          | string | 7 days before the present |
| `/explore_queries`                   | Explore Queries | User-defined [Explore query](#custom-explore-queries) streams. Each entry becomes its own incremental stream backed by Sentry's Explore events endpoint.                                                                                                          | array  |                           |
| `/advanced/window_size`              | Window Size  | Date window size for the `issues` backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours. If you have a significant amount of `isssues` data to backfill, smaller window sizes will allow the connector to checkpoint its progress more frequently. | string | P30D                      |

#### Custom Explore queries

Each entry in `/explore_queries` runs a [Sentry Explore events query](https://docs.sentry.io/api/explore/query-explore-events-in-table-format/) and is captured as its own incremental stream. The connector prefixes each stream name with `custom_explore_` and manages the capture time window itself.

| Property | Title | Description | Type | Required/Default |
| ---- | --- | --- | --- | --- |
| **`/explore_queries/-/name`** | Name | Name for this Explore query stream. The connector prefixes it with `custom_explore_` to form the stream name.  | string | Required |
| **`/explore_queries/-/dataset`** | Dataset  | Dataset to query. One of `spans`, `errors`, or `transactions`.  | string | Required |
| **`/explore_queries/-/fields`** | Fields | Comma-separated field list to return, e.g. `span.description, transaction, project`. The dataset's primary key fields and `timestamp` are added automatically. | string | Required |
| `/explore_queries/-/query` | Query | Optional Sentry search query to filter rows. The connector manages the time window itself, so a `timestamp:` clause is not allowed. | string | `""` |
| `/explore_queries/-/projects` | Projects | Comma-separated project IDs to include. Leave empty to query all projects. | string | `""` |

Refer to [Sentry's documentation](https://docs.sentry.io/api/explore/query-explore-events-in-table-format/) for the valid values for each field, including the field names and search query syntax supported by each dataset.

:::note
When `spans` are the dataset, only full fidelity data from the past 30 days are captured. Sentry reduces data earlier than 30 days ago to samples of the actual dataset.
:::

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
        image: ghcr.io/estuary/source-sentry:v2
        config:
          credentials:
            credentials_title: Private App Credentials
            access_token: <your auth token>
          organization: <your organization>
          start_date: "2025-08-14T00:00:00Z"
          explore_queries:
            - name: my_spans
              dataset: spans
              fields: span.description, span.duration, transaction, project
          advanced:
            window_size: P10D
    bindings:
      - resource:
          name: Issues
          interval: PT5M
        target: ${PREFIX}/Issues
```
