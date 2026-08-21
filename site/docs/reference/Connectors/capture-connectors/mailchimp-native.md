---
description: Capture Mailchimp lists, members, campaigns, and email activity with Estuary's native Mailchimp connector, using OAuth2 or a Mailchimp API key.
---

# Mailchimp

This connector captures data from Mailchimp into Estuary collections.
It authenticates with OAuth2 or a Mailchimp [API key](https://mailchimp.com/developer/marketing/guides/quick-start/#generate-your-api-key) and reads data through the [Mailchimp Marketing API](https://mailchimp.com/developer/marketing/api/).

## Supported data resources

The following data resources are supported:

| Resource                                                                                                           | Replication Mode |
| ------------------------------------------------------------------------------------------------------------------ | ---------------- |
| [automation_emails](https://mailchimp.com/developer/marketing/api/automation-email/list-automated-emails/)         | Full Refresh     |
| [automations](https://mailchimp.com/developer/marketing/api/automation/list-automations/)                          | Full Refresh     |
| [campaigns](https://mailchimp.com/developer/marketing/api/campaigns/list-campaigns/)                               | Incremental      |
| [email_activity](https://mailchimp.com/developer/marketing/api/email-activity-reports/list-email-activity/)        | Incremental      |
| [interest_categories](https://mailchimp.com/developer/marketing/api/interest-categories/list-interest-categories/) | Full Refresh     |
| [interests](https://mailchimp.com/developer/marketing/api/interests/list-interests-in-category/)                   | Full Refresh     |
| [list_members](https://mailchimp.com/developer/marketing/api/list-members/list-members-info/)                      | Incremental      |
| [lists](https://mailchimp.com/developer/marketing/api/lists/get-lists-info/)                                       | Full Refresh     |
| [segment_members](https://mailchimp.com/developer/marketing/api/list-segment-members/list-members-in-segment/)     | Full Refresh     |
| [segments](https://mailchimp.com/developer/marketing/api/list-segments/list-segments/)                             | Incremental      |
| [tags](https://mailchimp.com/developer/marketing/api/list-tag-search/search-for-tags-on-a-list-by-name/)           | Full Refresh     |

By default, each resource is mapped to an Estuary collection through a separate binding.

:::tip
The `campaigns` stream re-captures existing campaigns with periodic backfills, since Mailchimp's `/campaigns` endpoint only supports filtering by creation time while campaign records keep changing after creation (status transitions, report statistics). These backfills can be scheduled with the `schedule` resource config setting. By default, `campaigns`'s schedule is `0 0 * * *`, which means the stream attempts to backfill every day at midnight UTC. Campaign deletions are not captured.
:::

## Prerequisites

To set up the Mailchimp source connector, you'll need a Mailchimp [API key](https://mailchimp.com/developer/marketing/guides/quick-start/#generate-your-api-key), including its data center suffix (for example, a key ending in `-us21`). The connector uses the suffix to resolve your account's data-center-specific API endpoint automatically.

Alternatively, you can authenticate by signing in to Mailchimp with OAuth2 in the Estuary web app; the connector then resolves your API endpoint from Mailchimp's OAuth metadata endpoint.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Mailchimp source connector.

### Properties

#### Endpoint

The properties below reflect the manual, API key authentication method.

| Property                             | Title                 | Description                                                                                                                                                                                       | Type   | Required/Default |
| ------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/credentials/api_key`**           | API Key               | Your Mailchimp API key, including the data center suffix (e.g. ends in `-us21`).                                                                                                                  | string | Required         |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `API Key`.                                                                                                                                                    | string | Required         |
| `/start_date`                        | Start Date            | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string |                  |

#### Bindings

| Property    | Title             | Description                                                                                                                                                                                                                                                     | Type   | Required/Default |
| ----------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/name`** | Data resource     | Name of the data resource.                                                                                                                                                                                                                                      | string | Required         |
| `/interval` | Interval          | Interval between data syncs.                                                                                                                                                                                                                                    | string |                  |
| `/schedule` | Backfill schedule | The schedule for automatically backfilling this binding. Accepts a cron expression. For example, a schedule of `55 23 * * *` means the binding will initiate a new backfill at 23:55 UTC every day. If left empty, the binding will not automatically backfill. | string |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-mailchimp-native:v1
        config:
          credentials:
            credentials_title: API Key
            api_key: <secret>
          start_date: 2025-01-01T00:00:00Z
    bindings:
      - resource:
          name: lists
        target: ${PREFIX}/lists
      - resource:
          name: list_members
        target: ${PREFIX}/list_members
      - resource:
          name: campaigns
          schedule: 0 0 * * *
        target: ${PREFIX}/campaigns
      {...}
```
