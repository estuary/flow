# Zoho CRM

This connector captures data from Zoho CRM modules into Flow collections.
It uses Zoho's [Bulk API 2.0](https://www.zoho.com/crm/developer/docs/api/v7/bulk-read/overview.html) and [COQL API](https://www.zoho.com/crm/developer/docs/api/v7/COQL-Overview.html).

This connector offers several unique advantages:

- **Efficient Backfills**: Uses Zoho's Bulk API 2.0 for initial data loads and backfills, enabling significantly faster data transfer rates.

- **Real-time Incremental Updates**: Uses the COQL (CRM Object Query Language) API to efficiently detect and capture changes since the last sync.

- **Formula Field Handling**: The connector can automatically refresh formula fields on a configurable schedule. This ensures your formula field data stays current without manual intervention, even though Zoho doesn't track formula field changes in record modification timestamps.

This connector is available for use in the Flow web application.
For local development or open-source workflows, [`ghcr.io/estuary/source-zoho:dev`](https://ghcr.io/estuary/source-zoho:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data resources

This connector captures data from Zoho CRM [modules](https://www.zoho.com/crm/developer/docs/api/v7/modules-api.html). All available modules will appear after connecting to Zoho CRM.

:::info

Some modules are not supported by the API and cannot be captured:

- Notes
- Attachments
- Emails
- CTI_Entry
- Email_Analytics
- Email_Template_Analytics
- Functions__s
- Email_Sentiment
- Locking_Information__s

:::

## Prerequisites

### Authentication

This connector uses OAuth 2.0 to authenticate with Zoho CRM. You'll need to authorize Estuary to access your Zoho CRM data.

The connector requires the following OAuth scopes:

- `ZohoCRM.bulk.READ` - For Bulk Read API (backfills)
- `ZohoCRM.coql.READ` - For COQL queries (incremental updates)
- `ZohoCRM.modules.READ` - For reading module data
- `ZohoCRM.settings.modules.READ` - For reading module metadata
- `ZohoCRM.settings.fields.READ` - For field metadata

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Zoho CRM source connector.

### Properties

#### Endpoint

| Property                         | Title          | Description                                                                                                                                                      | Type   | Required/Default |
| -------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/credentials`**               | Authentication | OAuth2 credentials for Zoho CRM.                                                                                                                                 | object | Required         |
| **`/credentials/client_id`**     | Client ID      | The OAuth app's client ID.                                                                                                                                       | string | Required         |
| **`/credentials/client_secret`** | Client Secret  | The OAuth app's client secret.                                                                                                                                   | string | Required         |
| **`/credentials/refresh_token`** | Refresh Token  | The refresh token received from the OAuth app.                                                                                                                   | string | Required         |
| **`/credentials/api_domain`**    | API Domain     | The Zoho API domain. Automatically detected from the OAuth response.                                                                                             | string | Required         |
| `/start_date`                    | Start Date     | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Data added on and after this date will be captured. If left blank, defaults to 30 days before the present. | string | 30 days ago      |

#### Bindings

| Property    | Title                          | Description                                                                                                                                                                                                                                                                         | Type   | Required/Default |
| ----------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/name`** | Name                           | Name of the Zoho CRM module.                                                                                                                                                                                                                                                        | string | Required         |
| `/interval` | Interval                       | Interval between data syncs.                                                                                                                                                                                                                                                        | string | PT5M             |
| `/schedule` | Formula Field Refresh Schedule | The schedule for refreshing this binding's [formula fields](#formula-fields). Accepts a cron expression. For example, a schedule of `55 23 * * *` means the binding will refresh formula fields at 23:55 UTC every day. If left empty, the binding will not refresh formula fields. | string |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-zoho:dev
        config:
          credentials:
            client_id: <secret>
            client_secret: <secret>
            refresh_token: <secret>
            api_domain: https://www.zohoapis.com
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: Leads
          interval: PT5M
          schedule: "55 23 * * *"
        target: ${PREFIX}/Leads
      - resource:
          name: Accounts
          interval: PT5M
          schedule: "55 23 * * *"
        target: ${PREFIX}/Accounts
      - resource:
          name: Contacts
          interval: PT5M
        target: ${PREFIX}/Contacts
      {...}
```

## Formula Fields

Zoho CRM modules can contain [formula fields](https://help.zoho.com/portal/en/kb/crm/customize-crm-account/customizing-fields/articles/create-formula-fields), fields whose values are calculated based on other data. Since formula fields do not update the associated record's last modified timestamp when their values change, formula field updates are not incrementally captured by the connector.

To address this challenge, the Zoho CRM connector can refresh the values of formula fields on a schedule after the initial backfill completes. This is controlled at a binding level by the cron expression in the [`schedule` property](#bindings). When a scheduled formula field refresh occurs, the connector fetches every record's current formula field values and merges them into the associated collection with a top-level [`merge` reduction strategy](/reference/reduction-strategies/merge).
