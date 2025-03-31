
# GitHub

This connector captures data from GitHub repositories and organizations into Flow collections via GitHub's REST API.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-github:dev`](https://ghcr.io/estuary/source-github:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

When you [configure the connector](#endpoint), you specify a list of GitHub organizations and/or repositories
from which to capture data.

From your selection, the following data resources are captured:

| Full refresh (batch) resources | Incremental (real-time supported) resources |
|---|---|
| [Assignees](https://docs.github.com/en/rest/issues/assignees#list-assignees) | [Comments](https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository)|
| [Branches](https://docs.github.com/en/rest/branches/branches#list-branches)| [Commit comment reactions](https://docs.github.com/en/rest/reactions#list-reactions-for-a-commit-comment) |
| [Collaborators](https://docs.github.com/en/rest/collaborators/collaborators#list-repository-collaborators)| [Commit comments](https://docs.github.com/en/rest/commits/comments#list-commit-comments-for-a-repository) |
| [Issue labels](https://docs.github.com/en/rest/issues/labels#list-labels-for-a-repository)| [Commits](https://docs.github.com/en/rest/commits/commits#list-commits) |
| [Pull request commits](https://docs.github.com/en/rest/pulls/pulls#list-commits-on-a-pull-request)| [Deployments](https://docs.github.com/en/rest/deployments#list-deployments) |
| [Tags](https://docs.github.com/en/rest/repos/repos#list-repository-tags)| [Events](https://docs.github.com/en/rest/activity/events#list-repository-events) |
| [Team members](https://docs.github.com/en/rest/teams/members#list-team-members) | [Issue comment reactions](https://docs.github.com/en/rest/reactions#list-reactions-for-an-issue-comment)|
| [Team memberships](https://docs.github.com/en/rest/teams/members#get-team-membership-for-a-user)| [Issue events](https://docs.github.com/en/rest/issues#list-issue-events-for-a-repository) |
| [Teams](https://docs.github.com/en/rest/teams#list-teams) | [Issue milestones](https://docs.github.com/en/rest/issues#list-milestones) |
| [Users](https://docs.github.com/en/rest/orgs/members#list-organization-members) | [Issue reactions](https://docs.github.com/en/rest/reactions#list-reactions-for-an-issue) |
| | [Issues](https://docs.github.com/en/rest/issues/issues#list-repository-issues) |
| | [Project cards](https://docs.github.com/en/rest/projects/cards#list-project-cards)|
| | [Project columns](https://docs.github.com/en/rest/projects#list-project-columns) |
| | [Projects](https://docs.github.com/en/rest/projects/projects#list-repository-projects)|
| | [Pull request comment reactions](https://docs.github.com/en/rest/reactions#list-reactions-for-a-pull-request-review-comment)|
| | [Pull request stats](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request) |
| | [Pull requests](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests)|
| | [Releases](https://docs.github.com/en/rest/releases/releases#list-releases)|
| | [Repositories](https://docs.github.com/en/rest/repos/repos#list-organization-repositories) |
| | [Review comments](https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository)|
| | [Reviews](https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request)|
| | [Stargazers](https://docs.github.com/en/rest/activity/starring#list-stargazers)|
| | [Workflow runs](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-repository)|
| | [Workflows](https://docs.github.com/en/rest/actions/workflows#list-repository-workflows)|

Each resource is mapped to a Flow collection through a separate binding.

:::info
The `/start_date` [field](#endpoint) is not applicable to the following resources:
* Assignees
* Branches
* Collaborators
* Issue labels
* Organizations
* Pull request commits
* Pull request stats
* Repositories
* Tags
* Teams
* Users
:::

## Prerequisites

There are two ways to authenticate with GitHub when capturing data into Flow: using OAuth2, and manually, by generating a personal access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the access token method is the only supported method using the command line. Which authentication method you choose depends on the policies of your organization. Github has special organization settings that need to be enabled in order for users to be able to access repos that are part of an organization.

### Using OAuth2 to authenticate with GitHub in the Flow web app

* A GitHub user account with access to the repositories of interest, and which is a member of organizations of interest.

* User may need to request access in Github under the user's personal settings (not the organization settings) by going to Applications then Authorized OAuth Apps on Github. Click the app or the image next to the app and request access under "Organization access". After a user has made the request, the organization administrator can grant access on the "Third-party application access policy" page. See additional details on this [Github doc](https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-your-membership-in-organizations/requesting-organization-approval-for-oauth-apps).

### Configuring the connector specification manually using personal access token

* A GitHub user account with access to the repositories of interest, and which is a member of organizations of interest.

* A GitHub [personal access token](https://github.com/settings/tokens).
You may use multiple tokens to balance the load on your API quota.

* User may need to get the organization's administrator to grant access under "Third-party Access" then "Personal access tokens".

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the GitHub source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method. If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-github-in-the-flow-web-app), so some of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/branch` | Branch (Optional) | Space-delimited list of GitHub repository branches to pull commits for, e.g. `estuary/flow/your-branch`. If no branches are specified for a repository, the default branch will be pulled. | string |  |
| **`/credentials`** | Authentication | Choose how to authenticate to GitHub | object | Required |
| `/credentials/option_title` | Authentication method | Set to `PAT Credentials` for manual authentication | string |  |
| `/credentials/personal_access_token` | Access token | Personal access token, used for manual authentication. You may include multiple access tokens as a comma separated list. |
| `/page_size_for_large_streams` | Page size for large streams (Optional) | The Github connector captures from several resources with a large amount of data. The page size of such resources depends on the size of your repository. We recommended that you specify values between 10 and 30. | integer | `10` |
| **`/repository`** | GitHub Repositories | Space-delimited list of GitHub organizations/repositories, e.g. `estuary/flow` for a single repository, `estuary/*` to get all repositories from an organization and `estuary/flow estuary/another-repo` for multiple repositories. | string | Required |
| **`/start_date`** | Start date | The date from which you'd like to replicate data from GitHub in the format YYYY-MM-DDT00:00:00Z. For the resources that support this configuration, only data generated on or after the start date will be replicated. This field doesn't apply to all [resources](#supported-data-resources). | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | GitHub resource from which collection is captured. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-github:dev
          config:
            credentials:
              option_title: PAT Credentials
              personal_access_token: {secret}
            page_size_for_large_streams: 10
            repository: estuary/flow
            start_date: 2022-01-01T00:00:00Z
      bindings:
        - resource:
            stream: assignees
            syncMode: full_refresh
          target: ${PREFIX}/assignees
       {...}
```
