
# Outreach

This connector captures data from Outreach into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-outreach:dev`](https://ghcr.io/estuary/source-outreach:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Outreach API:

* [accounts](https://developers.outreach.io/api/reference/tag/Account/#tag/Account/paths/~1accounts/get)
* [calls](https://developers.outreach.io/api/reference/tag/Call/#tag/Call/paths/~1calls/get)
* [call_dispositions](https://developers.outreach.io/api/reference/tag/Call-Disposition/#tag/Call-Disposition/paths/~1callDispositions/get)
* [call_purposes](https://developers.outreach.io/api/reference/tag/Call-Purpose/#tag/Call-Purpose/paths/~1callPurposes/get)
* [email_addresses](https://developers.outreach.io/api/reference/tag/Email-Address/#tag/Email-Address/paths/~1emailAddresses/get)
* [events](https://developers.outreach.io/api/reference/tag/Event/#tag/Event/paths/~1events/get)
* [mailboxes](https://developers.outreach.io/api/reference/tag/Mailbox/#tag/Mailbox/paths/~1mailboxes/get)
* [mailings](https://developers.outreach.io/api/reference/tag/Mailing/#tag/Mailing/paths/~1mailings/get)
* [opportunities](https://developers.outreach.io/api/reference/tag/Opportunity/#tag/Opportunity/paths/~1opportunities/get)
* [opportunity_stages](https://developers.outreach.io/api/reference/tag/Opportunity-Stage/#tag/Opportunity-Stage/paths/~1opportunityStages/get)
* [prospects](https://developers.outreach.io/api/reference/tag/Prospect/#tag/Prospect/paths/~1prospects/get)
* [stages](https://developers.outreach.io/api/reference/tag/Stage/#tag/Stage/paths/~1stages/get)
* [tasks](https://developers.outreach.io/api/reference/tag/Task/#tag/Task/paths/~1tasks/get)
* [teams](https://developers.outreach.io/api/reference/tag/Team/#tag/Team/paths/~1teams/get)
* [templates](https://developers.outreach.io/api/reference/tag/Template/#tag/Template/paths/~1templates/get)
* [users](https://developers.outreach.io/api/reference/tag/User/#tag/User/paths/~1users/get)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

Authentication to Outreach is done via OAuth2.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Outreach source connector.

### Properties

#### Endpoint

The properties in the table below reflect manual authentication using the CLI. In the Flow web app,
you'll sign in directly and won't need the access token.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/start_date` | Start date | UTC date and time in the format 2025-04-24T00:00:00Z. Any data updated before this date will not be replicated. | string | 30 days before the present date |
| **`/credentials/access_token`** | Access Token | The access token received from the OAuth app. | string | Required |
| **`/credentials/access_token_expires_at`** | Access Token Expiration Datetime | The access token's expiration date and time in the format 2025-04-24T00:00:00Z. | string | Required |
| **`/credentials/client_id`** | OAuth Client ID | The OAuth app's client ID. | string | Required |
| **`/credentials/client_secret`** | OAuth Client Secret | The OAuth app's client secret. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `OAuth Credentials`. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | The refresh token received from the OAuth app. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | PT5M |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-outreach:dev
        config:
          credentials:
            access_token: <secret>
            access_token_expires_at: "2025-04-24T12:00:00Z"
            credentials_title: "OAuth Credentials"
            client_id: <secret>
            client_secret: <secret>
            refresh_token: <secret>
          start_date: "2025-04-24T12:00:00Z"
    bindings:
      - resource:
          name: accounts
        target: ${PREFIX}/accounts
```
