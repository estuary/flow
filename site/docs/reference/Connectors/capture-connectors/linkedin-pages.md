---
title: LinkedIn Pages
---

# LinkedIn Pages

This connector captures data from one LinkedIn Page into Flow collections via the [LinkedIn Marketing API](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/marketing-integrations-overview?view=li-lms-2024-03).

## Supported streams

- [Organization Lookup](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/organization-lookup-api?tabs=http#retrieve-organizations)
- [Follower Statistics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/follower-statistics?tabs=http#retrieve-lifetime-follower-statistics)
- [Share Statistics](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/share-statistics?tabs=http#retrieve-lifetime-share-statistics)
- [Total Follower Count](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/organization-lookup-api?tabs=http#retrieve-organization-follower-count)

## Getting started
The API user account should be assigned the `ADMIN` role and the following permissions for the API endpoints:
Endpoints such as: `Organization Lookup API`, `Follower Statistics` and `Share Statistics` require these permissions:
- `r_organization_social`: Read the organization's posts, comments, reactions, etc.
- `rw_organization_admin`: Write and read the organization's pages and read reporting data.

### Authentication
The authentication can be done by OAuth2.0 or by an Access Token, but we recommend OAuth2.0, as it allows for data streaming for a period of 12 months, compared to only 2 months with an access token.

#### Apply for API Access
1. Create a [LinkedIn Page](https://www.linkedin.com/help/linkedin/answer/a543852) if you don't have one.
2. Create a developer application in LinkedIn's Developer Portal [here](https://www.linkedin.com/developers/apps/new).
3. Ensure your application complies with the [Restricted Uses of LinkedIn Marketing APIs and Data](https://learn.microsoft.com/en-us/linkedin/marketing/restricted-use-cases?view=li-lms-2024-03).
4. [Apply](https://learn.microsoft.com/en-us/linkedin/marketing/increasing-access?view=li-lms-2024-03#increasing-access) to the Community Management under the Products tab of your LinkedIn app and complete the form.
5. Save your client_id and client_secret from the Auth Tab.

You can see more details about the Community Management App Review in [LinkedIn's Docs](https://learn.microsoft.com/en-us/linkedin/marketing/community-management-app-review?view=li-lms-2024-03).

#### Create the credentials
The connector can use either the OAuth2.0 method with `client_id`, `client_secret` and `refresh_token` or simply use an `access_token` in the UI connector's settings.

Access tokens expire after 60 days from their creation date, and you need to manually authenticate again after that.

Refresh tokens expire after 365 days from their creation date. If you receive a `401 invalid token response` error, it means that the access token has expired and you need to generate a new token. You can see more details about it in [LinkedIn's Docs](https://docs.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow).

1. Go to the LinkedIn Developers' [OAuth Token Tools](https://www.linkedin.com/developers/tools/oauth) and click **Create token**
2. Your app will need the `r_organization_social` and `rw_organization_admin` scopes:
3. Click "Request access token" and save the token.
