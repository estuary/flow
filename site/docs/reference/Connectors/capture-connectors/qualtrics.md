# Qualtrics

This connector captures data from Qualtrics surveys into Flow collections. It supports real-time data synchronization of surveys, survey questions, and survey responses from your Qualtrics account.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-qualtrics:dev`](https://ghcr.io/estuary/source-qualtrics:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The connector supports the following Qualtrics resources:

| Resource | Sync Mode | Update Interval | Description |
|----------|-----------|-----------------|-------------|
| [surveys](https://api.qualtrics.com/c3c1496b60e02-surveys) | Full Refresh | 15 minutes | Survey metadata including titles, settings, and configuration |
| [survey_questions](https://api.qualtrics.com/c3c1496b60e02-surveys) | Full Refresh | 15 minutes | Questions and structure for all surveys in your account |
| [survey_responses](https://api.qualtrics.com/97d9e758e373e-create-response-export) | Incremental | 5 minutes | Individual survey responses with all answer data |

## Prerequisites

To set up the Qualtrics source connector, you'll need:

1. **API Token**: Available in your Qualtrics account under **Account Settings > Qualtrics IDs > API** section
2. **Data Center ID**: Found in **Account Settings > Qualtrics IDs** (e.g., 'fra1', 'syd1', 'dub1')
3. **Appropriate permissions**: Your API token must have access to read surveys and export response data

## Configuration

You can configure the connector either in the Flow web app or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Qualtrics source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/credentials/credentials_title`** | Credentials Title | Name of the credentials set | string | Required, `"Private App Credentials"` |
| **`/credentials/access_token`** | API Token | Your Qualtrics API token found in Account Settings | string | Required |
| **`/data_center`** | Data Center | Your Qualtrics data center ID (e.g., 'fra1', 'syd1') | string | Required |
| `/start_date` | Start Date | UTC date and time for initial data fetch. Format: 2023-01-01T00:00:00Z | string | Default: 6 months ago |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/name`** | Resource Name | Name of the Qualtrics resource to capture | string | Required |
| **`/interval`** | Sync Interval | Interval between data syncs | string | See defaults above |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-qualtrics:dev
        config:
          credentials:
            credentials_title: Private App Credentials
            access_token: <secret>
          data_center: fra1
          start_date: "2023-01-01T00:00:00Z"
    bindings:
      - resource:
          name: surveys
          interval: PT15M
        target: ${PREFIX}/surveys
      - resource:
          name: survey_questions
          interval: PT15M
        target: ${PREFIX}/survey_questions
      - resource:
          name: survey_responses
          interval: PT5M
        target: ${PREFIX}/survey_responses
```
