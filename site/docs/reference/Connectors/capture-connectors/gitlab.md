
# GitLab

This connector captures data from GitLab repositories and organizations into Flow collections via GitLabs's API V4. It can also work with self-hosted GitLab.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-gitlab:dev`](https://ghcr.io/estuary/source-gitlab:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

When you [configure the connector](#configuration), you may provide a list of GitLab Groups or Projects from which to capture data.

From your selection, the following data resources are captured:

### Resources

 - [Branches](https://docs.gitlab.com/ee/api/branches.html)
 - [Commits](https://docs.gitlab.com/ee/api/commits.html)
 - [Issues](https://docs.gitlab.com/ee/api/issues.html)
 - [Group Issue Boards](https://docs.gitlab.com/ee/api/group_boards.html)
 - [Pipelines](https://docs.gitlab.com/ee/api/pipelines.html)
 - [Jobs](https://docs.gitlab.com/ee/api/jobs.html)
 - [Projects](https://docs.gitlab.com/ee/api/projects.html)
 - [Project Milestones](https://docs.gitlab.com/ee/api/milestones.html)
 - [Project Merge Requests](https://docs.gitlab.com/ee/api/merge_requests.html)
 - [Users](https://docs.gitlab.com/ee/api/users.html)
 - [Groups](https://docs.gitlab.com/ee/api/groups.html)
 - [Group Milestones](https://docs.gitlab.com/ee/api/group_milestones.html)
 - [Group and Project Members](https://docs.gitlab.com/ee/api/members.html)
 - [Tags](https://docs.gitlab.com/ee/api/tags.html)
 - [Releases](https://docs.gitlab.com/ee/api/releases/index.html)
 - [Group Labels](https://docs.gitlab.com/ee/api/group_labels.html)
 - [Project Labels](https://docs.gitlab.com/ee/api/labels.html)
 - [Epics](https://docs.gitlab.com/ee/api/epics.html) (only available for GitLab Ultimate and GitLab.com Gold accounts)
 - [Epic Issues](https://docs.gitlab.com/ee/api/epic_issues.html) (only available for GitLab Ultimate and GitLab.com Gold accounts)

Each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

There are two ways to authenticate with GitLab when capturing data into Flow: using OAuth2, and manually, by generating a personal access token.
Their prerequisites differ.

OAuth is recommended for simplicity in the Flow web app;
the access token method is the only supported method using the command line. Which authentication method you choose depends on the policies of your organization. GitLab has special organization settings that need to be enabled in order for users to be able to access repos that are part of an organization.

### Using OAuth2 to authenticate with GitLab in the Flow web app

* A GitLab user account with [access](https://docs.gitlab.com/ee/user/permissions.html) to the repositories of interest, and which is a member of organizations of interest.  [How to add a member](https://docs.gitlab.com/ee/user/project/members/#add-users-to-a-project).

### Configuring the connector specification manually using personal access token

* A GitLab user account with access to all entities of interest.

* A GitLab [personal access token](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the GitHub source connector.

### Setup

1. Complete authentication using Oauth or a PAT
2. Select your start date in the format 2023-08-31T00:00:00
3. Optionally select Groups and Projects

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-gitlab:dev
          config:
            credentials:
              option_title: PAT Credentials
              personal_access_token: {secret}
            groups: estuary.dev
            projects: estuary/flow
            start_date: 2022-01-01T00:00:00Z
      bindings:
        - resource:
            stream: branches
            syncMode: full_refresh
          target: ${PREFIX}/assignees
       {...}
```
