
# Jira

This connector captures data from [Jira's REST API](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-jira-native:dev`](https://ghcr.io/estuary/source-jira-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Jira APIs:

* [Issues](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-get)
* [Issue types](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-types/#api-group-issue-types)
* [Projects](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-search-get)
* [Users](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-users-search-get)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

- API Token: You can create an API token following [these steps from Jira](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/)
- Domain
- Email

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Jira source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/domain`** | Domain | The Domain for your Jira account, e.g. estuary.atlassian.net, estuary.jira.com, jira.your-domain.com | string | Required |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present. | string |  |
| **`/credentials/username`** | Email | The user email for your Jira account. | string | Required |
| **`/credentials/password`** | API Token | The value of the API token generated. | string | Required |
| `/advanced/projects` | Projects | Comma-separated list of project IDs from which to replicate issues. If left blank, issues from all projects wil be replicated. | string |  |

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
        image: ghcr.io/estuary/source-jira-native:dev
        config:
            advanced:
              projects: 12345,67890
            credentials:
              credentials: Email & API Token
              username: user@email.com
              password: <secret>
            domain: my.jira.domain
            start_date: "2025-05-23T00:00:00Z"
    bindings:
      - resource:
          name: issues
        target: ${PREFIX}/issues
```
