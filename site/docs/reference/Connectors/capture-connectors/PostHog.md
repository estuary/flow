# PostHog

This connector captures data from PostHog into Estuary collections.

This connector is available for use in the Estuary web application.
For local development or open-source workflows, [`ghcr.io/estuary/source-posthog:dev`](https://ghcr.io/estuary/source-posthog:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The connector captures the following PostHog resources:

**Snapshot resources** (full refresh each sync interval):

- Organizations
- Projects
- Persons
- Cohorts
- Annotations

**Incremental resources** (cursor-based change tracking):

- Feature Flags
- Events

:::info
The connector automatically discovers and captures data from all projects within the specified organization. You do not need to configure individual projects.
:::

## Prerequisites

- A PostHog account on [US Cloud](https://app.posthog.com), [EU Cloud](https://eu.posthog.com), or a self-hosted instance.

- A [Personal API Key](https://posthog.com/docs/api#personal-api-keys) with the appropriate scopes for the resources you want to capture:

  | Scope               | Resources       |
  | ------------------- | --------------- |
  | `cohort:read`       | Cohorts         |
  | `feature_flag:read` | Feature Flags   |
  | `annotation:read`   | Annotations     |
  | `query:read`        | Events, Persons |

  A wildcard scope grants access to all resources.

- Your **Organization ID** (UUID), which you can find in your PostHog organization settings.

:::info
Resources that your API key doesn't have the required scopes to access are automatically omitted during discovery.
:::

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the PostHog source connector.

### Properties

#### Endpoint

| Property                       | Title           | Description                                                                                                                                                      | Type   | Required/Default          |
| ------------------------------ | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------------- |
| **`/credentials`**             | Authentication  | Personal API Key credentials for PostHog.                                                                                                                        | object | Required                  |
| **`/credentials/credentials`** | API Key         | PostHog Personal API Key.                                                                                                                                        | string | Required                  |
| **`/organization_id`**         | Organization ID | UUID of the PostHog organization to capture data from. The connector captures from all projects within this organization.                                        | string | Required                  |
| `/base_url`                    | Base URL        | PostHog API base URL. Use `https://app.posthog.com` for US Cloud, `https://eu.posthog.com` for EU Cloud, or a custom URL for self-hosted instances.              | string | `https://app.posthog.com` |
| `/start_date`                  | Start Date      | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Data added on and after this date will be captured. If left blank, defaults to 30 days before the present. | string | 30 days ago               |

#### Bindings

| Property    | Title    | Description                   | Type   | Required/Default |
| ----------- | -------- | ----------------------------- | ------ | ---------------- |
| **`/name`** | Name     | Name of the PostHog resource. | string | Required         |
| `/interval` | Interval | Interval between data syncs.  | string | PT5M             |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-posthog:dev
        config:
          credentials:
            credentials: <secret>
          organization_id: "your-organization-uuid"
          base_url: https://app.posthog.com
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: Events
          interval: PT5M
        target: ${PREFIX}/Events
      - resource:
          name: Persons
          interval: PT5M
        target: ${PREFIX}/Persons
      - resource:
          name: FeatureFlags
          interval: PT5M
        target: ${PREFIX}/FeatureFlags
      {...}
```
