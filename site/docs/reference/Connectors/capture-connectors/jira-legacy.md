
# Jira (legacy)

This connector captures data from [Jira's REST API](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/) into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-jira-legacy:dev`](https://ghcr.io/estuary/source-jira-legacy:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Jira APIs:

* [Application roles](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-application-roles/#api-rest-api-3-applicationrole-get)
* [Avatars](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-avatars/#api-rest-api-3-avatar-type-system-get)
* [Boards](https://developer.atlassian.com/cloud/jira/software/rest/api-group-other-operations/#api-agile-1-0-board-get)
* [Board issues](https://developer.atlassian.com/cloud/jira/software/rest/api-group-board/#api-rest-agile-1-0-board-boardid-issue-get)
* [Dashboards](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-dashboards/#api-rest-api-3-dashboard-get)
* [Filters](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-filters/#api-rest-api-3-filter-search-get)
* [Filter sharing](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-filter-sharing/#api-rest-api-3-filter-id-permission-get)
* [Groups](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-groups/#api-rest-api-3-groups-picker-get)
* [Issues](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-get)
* [Issue comments](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get)
* [Issue fields](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-fields/#api-rest-api-3-field-get)
* [Issue field configurations](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-field-configurations/#api-rest-api-3-fieldconfiguration-get)
* [Issue custom field contexts](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-contexts/#api-rest-api-3-field-fieldid-context-get)
* [Issue custom field options](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-custom-field-options/#api-rest-api-3-field-fieldid-context-contextid-option-get)
* [Issue link types](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-link-types/#api-rest-api-3-issuelinktype-get)
* [Issue navigator settings](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-navigator-settings/#api-rest-api-3-settings-columns-get)
* [Issue notification schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-notification-schemes/#api-rest-api-3-notificationscheme-get)
* [Issue priorities](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-priorities/#api-rest-api-3-priority-get)
* [Issue properties](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-properties/#api-rest-api-3-issue-issueidorkey-properties-propertykey-get)
* [Issue remote links](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-remote-links/#api-rest-api-3-issue-issueidorkey-remotelink-get)
* [Issue resolutions](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-resolutions/#api-rest-api-3-resolution-search-get)
* [Issue security schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-security-schemes/#api-rest-api-3-issuesecurityschemes-get)
* [Issue transitions](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-transitions-get)
* [Issue types](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-types/#api-group-issue-types)
* [Issue type schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-type-schemes/#api-rest-api-3-issuetypescheme-get)
* [Issue type screen schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-type-screen-schemes/#api-rest-api-3-issuetypescreenscheme-get)
* [Issue votes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-votes/#api-group-issue-votes)
* [Issue watchers](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-watchers/#api-rest-api-3-issue-issueidorkey-watchers-get)
* [Issue worklogs](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-worklogs/#api-rest-api-3-issue-issueidorkey-worklog-get)
* [Jira settings](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-jira-settings/#api-rest-api-3-application-properties-get)
* [Labels](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-labels/#api-rest-api-3-label-get)
* [Permissions](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-permissions/#api-rest-api-3-mypermissions-get)
* [Permission schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-permission-schemes/#api-rest-api-3-permissionscheme-get)
* [Projects](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-search-get)
* [Project avatars](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-avatars/#api-rest-api-3-project-projectidorkey-avatars-get)
* [Project categories](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-categories/#api-rest-api-3-projectcategory-get)
* [Project components](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-components/#api-rest-api-3-project-projectidorkey-component-get)
* [Project email](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-email/#api-rest-api-3-project-projectid-email-get)
* [Project permission schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-permission-schemes/#api-group-project-permission-schemes)
* [Project roles](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-roles#api-rest-api-3-role-get)
* [Project types](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-types/#api-rest-api-3-project-type-get)
* [Project versions](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-project-versions/#api-rest-api-3-project-projectidorkey-version-get)
* [Screens](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screens/#api-rest-api-3-screens-get)
* [Screen tabs](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screen-tabs/#api-rest-api-3-screens-screenid-tabs-get)
* [Screen tab fields](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screen-tab-fields/#api-rest-api-3-screens-screenid-tabs-tabid-fields-get)
* [Screen schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-screen-schemes/#api-rest-api-3-screenscheme-get)
* [Sprints](https://developer.atlassian.com/cloud/jira/software/rest/api-group-board/#api-rest-agile-1-0-board-boardid-sprint-get)
* [Sprint issues](https://developer.atlassian.com/cloud/jira/software/rest/api-group-sprint/#api-rest-agile-1-0-sprint-sprintid-issue-get)
* [Time tracking](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-time-tracking/#api-rest-api-3-configuration-timetracking-list-get)
* [Users](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-user-search/#api-rest-api-3-user-search-get)
* [UsersGroupsDetailed](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get)
* [Workflows](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflows/#api-rest-api-3-workflow-search-get)
* [Workflow schemes](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflow-schemes/#api-rest-api-3-workflowscheme-get)
* [Workflow statuses](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflow-statuses/#api-rest-api-3-status-get)
* [Workflow status categories](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflow-status-categories/#api-rest-api-3-statuscategory-get)

By default, each resource is mapped to a Flow collection through a separate binding.

### Experimental streams

These resources are not documented by Jira and must specifically be enabled in the connector configuration.

* Pull Requests (GitHub pull requests linked to issues)

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
| `/api_token` | API Token | Jira API Token is used for Authorization to your account by BasicAuth. | string | Required |
| `/domain` | Domain | The Domain for your Jira account, e.g. estuary.atlassian.net, estuary.jira.com, jira.your-domain.com | string | Required |
| `/email` | Email | The user email for your Jira account which you used to generate the API token. This field is used for Authorization to your account by BasicAuth. | string | Required |
| `/projects` | Projects | List of Jira project keys to replicate data for. Leave it empty to replicate data for all projects. | string[] |  |
| `/start_date` | Start Date | UTC date-time in the format `YYYY-MM-DDT00:00:00Z`. Data generated before this date will not be replicated. Note that this field only applies to certain streams. | string |  |
| `/lookback_window_minutes` | Lookback Window | When set to N, the connector will always refresh resources created within the past N minutes. | integer | `0` |
| `/enable_experimental_streams` | Enable Experimental Streams | Allow the use of experimental streams which rely on undocumented Jira API endpoints. See [experimental streams](#experimental-streams) above. | boolean | `false` |
| `/expand_issue_changelog` | Expand Issue Changelog | (Deprecated) Expand the changelog when replicating issues. See `issues_stream_expand_with` for a newer implementation. | boolean | `false` |
| `/render_fields` | Render Issue Fields | (Deprecated) Render issue fields in HTML format in addition to Jira JSON-like format. See `issues_stream_expand_with` for a newer implementation. | boolean | `false` |
| `/expand_issue_transition` | Expand Issue Transitions | (Deprecated) Expand the transitions when replicating issues. See `issues_stream_expand_with` for a newer implementation. | boolean | `false` |
| `/issues_stream_expand_with` | Expand Issues Stream | Array that compiles issue expansion options. The array can include these enum values: `renderedFields`, `transitions`, `changelog`. | string[] | `[]` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Jira project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-jira-legacy:dev
        config:
          api_token: <token>
          domain: <domain>
          email: <email>
          start_date: 2025-01-01T00:00:00Z
          lookback_window_minutes: 60
          enable_experimental_streams: false
          issues_stream_expand_with:
            - renderedFields
            - transitions
    bindings:
      - resource:
          stream: issues
          syncMode: incremental
        target: ${PREFIX}/issues
      {...}
```
