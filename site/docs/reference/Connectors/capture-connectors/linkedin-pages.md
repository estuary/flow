

# LinkedIn Pages

This connector captures data from one LinkedIn Page into Flow collections via the [LinkedIn Marketing API](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/marketing-integrations-overview?view=li-lms-2024-03).

## Prerequisites

* An existing LinkedIn Account

The API user account should be assigned the `ADMIN` role and the following permissions for the API endpoints.
Endpoints such as: `Organization Lookup API`, `Follower Statistics` and `Share Statistics` require these permissions:
- `r_organization_social`: Read the organization's posts, comments, reactions, etc.
- `rw_organization_admin`: Write and read the organization's pages and read reporting data.

### Authentication
This connector's authentication can be configured by either passing a LinkedIn access token or using Oauth to connect to your source. Oauth requires the additional setup of verifying your application with LinkedIn in order to use the required scopes.

You can see more details about the Community Management App Review in [LinkedIn's Docs](https://learn.microsoft.com/en-us/linkedin/marketing/community-management-app-review?view=li-lms-2024-03).

:::info
LinkedIn access tokens expire in 60 days.
You must manually update your capture configuration to continue to capture data from LinkedIn.

Refresh tokens expire after 365 days from their creation date. If you receive a `401 invalid token response` error, it means that the access token has expired and you need to generate a new token. You can see more details about it in [LinkedIn's Docs](https://docs.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow).
:::

### Access Token Authentication

1. Go to the LinkedIn Developers' [OAuth Token Tools](https://www.linkedin.com/developers/tools/oauth) and click **Create token**
2. Your app will need the `r_organization_social` and `rw_organization_admin` scopes:
3. Click "Request access token" and save the token.

### Oauth Authentication

#### Create a LinkedIn OAuth App
1. Create a [LinkedIn Page](https://www.linkedin.com/help/linkedin/answer/a543852) if you don't have one.
2. [Create](https://www.linkedin.com/developers/apps/new) a developer application in LinkedIn's Developer Portal.
3. Ensure your application complies with the [Restricted Uses of LinkedIn Marketing APIs and Data](https://learn.microsoft.com/en-us/linkedin/marketing/restricted-use-cases?view=li-lms-2024-03).
4. [Apply](https://learn.microsoft.com/en-us/linkedin/marketing/increasing-access?view=li-lms-2024-03#increasing-access) to the Community Management API under the Products tab of your LinkedIn app and complete the form.
5. Save your `client_id` and `client_secret` from the Auth tab.

#### Create a Refresh Token
1. Go to the LinkedIn Developers' [OAuth Token Tools](https://www.linkedin.com/developers/tools/oauth) and click **Create token**
2. Your app will need the `r_organization_social` and `rw_organization_admin` scopes:
3. Click "Request access token" and save the token.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the LinkedIn Pages source connector.

### Properties

| Property           | Title           | Description                                                                                                                   | Type   | Required/Default |
| ------------------ | --------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| `/organization_id` | Organization ID | Your unique organization's id, found in the url of your bussiness' Organization Page                                          | string | Required         |
| `/client_id`       | Client ID       | Your Oauth app's client id.                                                                                                   | string | Required         |
| `/client_secret`   | Client Secret   | Your Oauth app's client secret.                                                                                               | string | Required         |
| `/refresh_token`   | Refresh Token   | The token value generated using the LinkedIn Developers [OAuth Token Tools](https://www.linkedin.com/developers/tools/oauth). | string | Required         |
| `/access_token`    | Access Token    | The token value generated using the LinkedIn Developers [OAuth Token Tools](https://www.linkedin.com/developers/tools/oauth). | string | Required         |


## Supported Streams

- [Organization Lookup](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/organization-lookup-api?tabs=http#retrieve-organizations)
- [Follower Statistics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/follower-statistics?tabs=http#retrieve-lifetime-follower-statistics)
- [Share Statistics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/share-statistics?tabs=http#retrieve-lifetime-share-statistics)
- [Total Follower Count](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/organization-lookup-api?tabs=http#retrieve-organization-follower-count)
