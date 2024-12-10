# SendGrid

This connector captures data from SendGrid into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sendgrid:dev`](https://ghcr.io/estuary/source-sendgrid:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported through the SendGrid APIs:

* [Campaigns](https://docs.sendgrid.com/api-reference/campaigns-api/retrieve-all-campaigns)
* [Lists](https://docs.sendgrid.com/api-reference/lists/get-all-lists)
* [Contacts](https://docs.sendgrid.com/api-reference/contacts/export-contacts)
* [Stats automations](https://docs.sendgrid.com/api-reference/marketing-campaign-stats/get-all-automation-stats)
* [Segments](https://docs.sendgrid.com/api-reference/segmenting-contacts/get-list-of-segments)
* [Single Sends](https://docs.sendgrid.com/api-reference/marketing-campaign-stats/get-all-single-sends-stats)
* [Templates](https://docs.sendgrid.com/api-reference/transactional-templates/retrieve-paged-transactional-templates)
* [Global suppression](https://docs.sendgrid.com/api-reference/suppressions-global-suppressions/retrieve-all-global-suppressions)
* [Suppression groups](https://docs.sendgrid.com/api-reference/suppressions-unsubscribe-groups/retrieve-all-suppression-groups-associated-with-the-user)
* [Suppression group members](https://docs.sendgrid.com/api-reference/suppressions-suppressions/retrieve-all-suppressions)
* [Blocks](https://docs.sendgrid.com/api-reference/blocks-api/retrieve-all-blocks)
* [Bounces](https://docs.sendgrid.com/api-reference/bounces-api/retrieve-all-bounces)
* [Invalid emails](https://docs.sendgrid.com/api-reference/invalid-e-mails-api/retrieve-all-invalid-emails)
* [Spam reports](https://docs.sendgrid.com/api-reference/spam-reports-api/retrieve-all-spam-reports)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* SendGrid [API Key](https://docs.sendgrid.com/api-reference/api-keys/create-api-keys) for authentication.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SendGrid source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/apikey` | Sendgrid API key | The value of the SendGrid API Key generated. | string | Required |
| `/start_date` | Start Date | The date from which you'd like to replicate data for SendGrid API, in the format YYYY-MM-DDT00:00:00Z. Any data before this date will not be replicated. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your SendGrid project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sendgrid:dev
        config:
          apikey: <secret>
          start_date: 2017-01-25T00:00:00Z
    bindings:
      - resource:
          stream: blocks
          syncMode: incremental
        target: ${PREFIX}/blocks
      {...}
```