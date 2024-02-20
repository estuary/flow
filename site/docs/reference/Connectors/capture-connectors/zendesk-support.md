# Zendesk Support

This connector captures data from Zendesk into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-zendesk-support:dev`](https://ghcr.io/estuary/source-zendesk-support:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.
You can find their documentation [here](https://docs.airbyte.com/integrations/sources/zendesk-support/),
but keep in mind that the two versions may be significantly different.

## Supported data resources

The following data resources are supported through the Zendesk API:

* [Brands](https://developer.zendesk.com/api-reference/ticketing/account-configuration/brands/)
* [Custom roles](https://developer.zendesk.com/api-reference/ticketing/account-configuration/custom_roles/)
* [Group memberships](https://developer.zendesk.com/api-reference/ticketing/groups/group_memberships/)
* [Groups](https://developer.zendesk.com/api-reference/ticketing/groups/groups/)
* [Macros](https://developer.zendesk.com/api-reference/ticketing/business-rules/macros/)
* [Organizations](https://developer.zendesk.com/api-reference/ticketing/organizations/organizations/)
* [Satisfaction ratings](https://developer.zendesk.com/api-reference/ticketing/ticket-management/satisfaction_ratings/)
* [Schedules](https://developer.zendesk.com/api-reference/ticketing/ticket-management/schedules/)
* [SLA policies](https://developer.zendesk.com/api-reference/ticketing/business-rules/sla_policies/)
* [Tags](https://developer.zendesk.com/api-reference/ticketing/ticket-management/tags/)
* [Ticket audits](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_audits/)
* [Ticket comments](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_comments/)
* [Ticket fields](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_fields/)
* [Ticket forms](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_forms/)
* [Ticket metrics](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metrics/)
* [Ticket metric events](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metric_events/)
* [Tickets](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-ticket-export-time-based)
* [Users](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-user-export)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

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
| `/credentials/api_token` | API Token | The value of the API token generated. | string |  |
| `/credentials/credentials` | Credentials method | Type of credentials used. Set to `api-token` | string |  |
| `/credentials/email` | Email | The user email for your Zendesk account. | string |  |
| **`/start_date`** | Start Date | The date from which you&#x27;d like to replicate data for Zendesk Support API, in the format YYYY-MM-DDT00:00:00Z. All data generated after this date will be replicated. | string | Required |
| **`/subdomain`** | Subdomain | This is your Zendesk subdomain that can be found in your account URL. For example, in `https://MY_SUBDOMAIN.zendesk.com/`, where `MY_SUBDOMAIN` is the value of your subdomain. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource in Zendesk from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-zendesk-support:dev
        config:
            credentials:
              api_token: <secret>
              credentials: api_token
              email: user@domain.com
            start_date: 2022-03-01T00:00:00Z
            subdomain: my_subdomain
    bindings:
      - resource:
          stream: group_memberships
          syncMode: incremental
        target: ${PREFIX}/groupmemberships
      - resource:
          stream: groups
          syncMode: incremental
        target: ${PREFIX}/groups
      - resource:
          stream: macros
          syncMode: incremental
        target: ${PREFIX}/macros
      - resource:
          stream: organizations
          syncMode: incremental
        target: ${PREFIX}/organizations
      - resource:
          stream: satisfaction_ratings
          syncMode: incremental
        target: ${PREFIX}/satisfactionratings
      - resource:
          stream: sla_policies
          syncMode: full_refresh
        target: ${PREFIX}/slapoliciies
      - resource:
          stream: tags
          syncMode: full_refresh
        target: ${PREFIX}/tags
      - resource:
          stream: ticket_audits
          syncMode: incremental
        target: ${PREFIX}/ticketaudits
      - resource:
          stream: ticket_comments
          syncMode: incremental
        target: ${PREFIX}/ticketcomments
      - resource:
          stream: ticket_fields
          syncMode: incremental
        target: ${PREFIX}/ticketfields
      - resource:
          stream: ticket_forms
          syncMode: incremental
        target: ${PREFIX}/ticketforms
      - resource:
          stream: ticket_metrics
          syncMode: incremental
        target: ${PREFIX}/ticketmetrics
      - resource:
          stream: ticket_metric_events
          syncMode: incremental
        target: ${PREFIX}/ticketmetricevents
      - resource:
          stream: tickets
          syncMode: incremental
        target: ${PREFIX}/tickets
      - resource:
          stream: users
          syncMode: incremental
        target: ${PREFIX}/users
      - resource:
          stream: brands
          syncMode: full_refresh
        target: ${PREFIX}/brands
      - resource:
          stream: custom_roles
          syncMode: full_refresh
        target: ${PREFIX}/customroles
      - resource:
          stream: schedules
          syncMode: full_refresh
        target: ${PREFIX}/schedules
```
