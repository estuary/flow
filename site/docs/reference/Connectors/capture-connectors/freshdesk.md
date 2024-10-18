
# Freshdesk

This connector captures Freshdesk data into Flow collections via the [Freshdesk API](https://developers.freshdesk.com/api/#introduction).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-freshdesk:dev`](https://ghcr.io/estuary/source-freshdesk:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

* [Agents](https://developers.freshdesk.com/api/#agents)
* [Business hours](https://developers.freshdesk.com/api/#business-hours)
* [Canned response folders](https://developers.freshdesk.com/api/#list_all_canned_response_folders)
* [Canned responses](https://developers.freshdesk.com/api/#canned-responses)
* [Companies](https://developers.freshdesk.com/api/#companies)
* [Contacts](https://developers.freshdesk.com/api/#contacts)
* [Conversations](https://developers.freshdesk.com/api/#conversations)
* [Discussion categories](https://developers.freshdesk.com/api/#category_attributes)
* [Discussion comments](https://developers.freshdesk.com/api/#comment_attributes)
* [Discussion forums](https://developers.freshdesk.com/api/#forum_attributes)
* [Discussion topics](https://developers.freshdesk.com/api/#topic_attributes)
* [Email configs](https://developers.freshdesk.com/api/#email-configs)
* [Email mailboxes](https://developers.freshdesk.com/api/#email-mailboxes)
* [Groups](https://developers.freshdesk.com/api/#groups)
* [Products](https://developers.freshdesk.com/api/#products)
* [Roles](https://developers.freshdesk.com/api/#roles)
* [Satisfaction ratings](https://developers.freshdesk.com/api/#satisfaction-ratings)
* [Scenario automations](https://developers.freshdesk.com/api/#scenario-automations)
* [Settings](https://developers.freshdesk.com/api/#settings)
* [Skills](https://developers.freshdesk.com/api/#skills)
* [SLA policies](https://developers.freshdesk.com/api/#sla-policies)
* [Solution articles](https://developers.freshdesk.com/api/#solution_article_attributes)
* [Solution categories](https://developers.freshdesk.com/api/#solution_category_attributes)
* [Solution folders](https://developers.freshdesk.com/api/#solution_folder_attributes)
* [Surveys](https://developers.freshdesk.com/api/#surveys)
* [Ticket fields](https://developers.freshdesk.com/api/#ticket-fields)
* [Tickets](https://developers.freshdesk.com/api/#tickets)
* [Time entries](https://developers.freshdesk.com/api/#time-entries)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To use this connector, you'll need:
* Your [Freshdesk account URL](https://support.freshdesk.com/en/support/solutions/articles/237264-how-do-i-find-my-freshdesk-account-url-using-my-email-address-)
* Your [Freshdesk API key](https://support.freshdesk.com/en/support/solutions/articles/215517)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Freshdesk source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/api_key`** | API Key | [Freshdesk API Key.](https://support.freshdesk.com/en/support/solutions/articles/215517) | string | Required |
| **`/domain`** | Domain | Freshdesk domain | string | Required |
| `/requests_per_minute` | Requests per minute | The number of requests per minute that this source is allowed to use. There is a rate limit of 50 requests per minute per app per account. | integer |  |
| `/start_date` | Start Date | UTC date and time. Any data created after this date will be replicated. If this parameter is not set, all data will be replicated. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource from the Freshdesk API from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-freshdesk:dev
        config:
            api_key: xxxxxxxxxxxxxxxx
            domain: acmesupport.freshdesk.com
    bindings:
      - resource:
          stream: agents
          syncMode: incremental
        target: ${PREFIX}/agents
      {...}
```
