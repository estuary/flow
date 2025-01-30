# Zendesk Support Real-Time

This connector captures data from Zendesk into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-zendesk-support-native:dev`](https://ghcr.io/estuary/source-zendesk-support-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Zendesk API:

* [Audit logs](https://developer.zendesk.com/api-reference/ticketing/account-configuration/audit_logs/#list-audit-logs)
* [Brands](https://developer.zendesk.com/api-reference/ticketing/account-configuration/brands/)
* [Groups](https://developer.zendesk.com/api-reference/ticketing/groups/groups/)
* [Macros](https://developer.zendesk.com/api-reference/ticketing/business-rules/macros/)
* [Organization memberships](https://developer.zendesk.com/api-reference/ticketing/organizations/organization_memberships/)
* [Satisfaction ratings](https://developer.zendesk.com/api-reference/ticketing/ticket-management/satisfaction_ratings/)
* [Tags](https://developer.zendesk.com/api-reference/ticketing/ticket-management/tags/)
* [Ticket audits](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_audits/)
* [Ticket comments](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_comments/)
* [Ticket fields](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_fields/)
* [Ticket metrics](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metrics/)
* [Ticket metric events](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metric_events/)
* [Ticket skips](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_skips/)
* [Tickets](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-ticket-export-cursor-based)
* [Users](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-user-export-cursor-based)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

There are two different ways to authenticate with Zendesk Support when capturing data into Flow: using OAuth2 or providing an API token. The prerequisites for both authentication methods are listed below.

### OAuth2 authentication

* Subdomain of your Zendesk URL. In the URL `https://MY_SUBDOMAIN.zendesk.com/`, `MY_SUBDOMAIN` is the subdomain.

### API token authentication

* Subdomain of your Zendesk URL. In the URL `https://MY_SUBDOMAIN.zendesk.com/`, `MY_SUBDOMAIN` is the subdomain.
* Email address associated with your Zendesk account.
* A Zendesk API token. See the [Zendesk docs](https://support.zendesk.com/hc/en-us/articles/4408889192858-Generating-a-new-API-token) to enable tokens and generate a new token.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Zendesk Support source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/subdomain`** | Subdomain | This is your Zendesk subdomain that can be found in your account URL. For example, in `https://MY_SUBDOMAIN.zendesk.com/`, where `MY_SUBDOMAIN` is the value of your subdomain. | string | Required |
| **`/start_date`** | Start Date | The date from which you&#x27;d like to replicate data for Zendesk Support API, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated. | string | Required |
| `/credentials/username` | Email | The user email for your Zendesk account. | string | Required for API token authentication |
| `/credentials/password` | API Token | The value of the API token generated. | string | Required for API token authentication |
| `/credentials/client_id` | OAuth Client ID | The OAuth app's client ID. | string | Required for OAuth2 authentication |
| `/credentials/client_secret` | OAuth Client Secret | The OAuth app's client secret. | string | Required for OAuth2 authentication |
| `/credentials/access_token` | Access Token | The access token received from the OAuth app. | string | Required for OAuth2 authentication |
| `/advanced/incremental_export_page_size` | Incremental Export Streams' Page Size | Page size for incremental export streams. Typically left as the default unless Estuary Support or the connector logs indicate otherwise. | integer | 1,000 |


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
        image: ghcr.io/estuary/source-zendesk-support-native:dev
        config:
            advanced:
              incremental_export_page_size: 1000
            credentials:
              credentials: Email & API Token
              username: user@domain.com
              password: <secret>
            start_date: "2025-01-30T00:00:00Z"
            subdomain: my_subdomain
    bindings:
      - resource:
          name: tickets
        target: ${PREFIX}/tickets
```
