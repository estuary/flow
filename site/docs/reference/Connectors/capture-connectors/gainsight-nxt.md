# Gainsight NXT

This connector captures data from [Gainsight NXT](https://www.gainsight.com/), a customer success platform. It uses Gainsight's REST API to capture data in real-time.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-gainsight-nxt:dev`](https://github.com/estuary/estuary/pkgs/container/source-gainsight-nxt) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past images.

## Supported Streams

The connector supports the following Gainsight NXT resources.

| Stream               | Capture Type | Cursor           |
| -------------------- | ------------ | ---------------- |
| `da_picklist`        | full-refresh | N/A              |
| `companies`          | incremental  | ModifiedDate     |
| `users`              | incremental  | ModifiedDate     |
| `success_plans`      | incremental  | ModifiedDate     |
| `cs_tasks`           | incremental  | ModifiedDate     |
| `activity_timelines` | incremental  | LastModifiedDate |
| `call_to_actions`    | incremental  | ModifiedDate     |

## Prerequisites

To set up the Gainsight source connector, you'll need:

- A Gainsight NXT [API Access Key](https://support.gainsight.com/gainsight_nxt/Connectors/Connectors_Version_-_Archived/03API_Integration/Generate_API_Access_Key)
- Your Gainsight domain

## Configuration

You can configure the connector either in the Flow web app or by directly editing the catalog specification file. See connectors to learn more about using [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors). The values and specification sample below provide configuration details specific to the Gainsight NXT source connector.

### Properties

#### Endpoint

| Property                         | Title             | Description                                                                                             | Type   | Required/Default                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------- | ------ | ------------------------------------- |
| `/credentials/access_token`      | API Access Key    | Gainsight API access key for authentication.                                                            | string | Required                              |
| `/credentials/credentials_title` | Credentials Title | Name of the credentials set.                                                                            | string | Required, `"Private App Credentials"` |
| `/domain`                        | Domain            | The domain for your Gainsight instance.                                                                 | string | Required                              |
| `/start_date`                    | Start Date        | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required, `30 day ago`                |

#### Bindings

| Property        | Title         | Description                                                   | Type   | Required/Default |
| --------------- | ------------- | ------------------------------------------------------------- | ------ | ---------------- |
| **`/name`**     | Resource Name | Name of the Gainsight resource to capture.                    | string | Required         |
| **`/interval`** | Sync Interval | Interval between data syncs (e.g., PT2M for every 2 minutes). | string |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gainsight-nxt:dev
        config:
          credentials:
            credentials_title: Private App Credentials
            api_token: <secret>
          domain: yourdomain
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: companies
        target: ${PREFIX}/companies
      - resource:
          name: users
        target: ${PREFIX}/users
```
