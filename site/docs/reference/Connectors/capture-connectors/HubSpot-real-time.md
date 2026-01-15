# HubSpot ( Real-Time )

This connector captures data from HubSpot into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-hubspot-native:dev`](https://ghcr.io/estuary/source-hubspot-native:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported HubSpot Resources

The connector automatically discovers bindings for the following HubSpot resources:

* [Companies](https://developers.hubspot.com/docs/api/crm/companies)
* [Contact List Memberships](https://developers.hubspot.com/docs/api-reference/crm-lists-v3/guide#manage-list-membership)
* [Contact Lists](https://developers.hubspot.com/docs/api-reference/crm-lists-v3/guide)
* [Contacts](https://developers.hubspot.com/docs/api/crm/contacts)
* [Custom Objects](https://developers.hubspot.com/docs/api/crm/crm-custom-objects)
* [Deal Pipelines](https://developers.hubspot.com/beta-docs/guides/api/crm/pipelines)
* [Deals](https://developers.hubspot.com/docs/api/crm/deals)
* [Email Events](https://developers.hubspot.com/docs/methods/email/get_events)
* [Engagements](https://developers.hubspot.com/docs/api/crm/engagements)
* [Feedback Submissions](https://developers.hubspot.com/docs/api/crm/feedback-submissions)
* [Form Submissions](https://developers.hubspot.com/docs/reference/api/marketing/forms/v1)
* [Forms](https://developers.hubspot.com/docs/reference/api/marketing/forms/v3)
* [Goals](https://developers.hubspot.com/docs/api-reference/crm-goal-targets-v3/guide)
* [Line Items](https://developers.hubspot.com/beta-docs/guides/api/crm/objects/line-items)
* [Marketing Emails](https://developers.hubspot.com/docs/api-reference/marketing-marketing-emails-v3/marketing-emails/get-marketing-v3-emails-)
* [Orders](https://developers.hubspot.com/docs/api-reference/crm-orders-v3/guide)
* [Owners](https://developers.hubspot.com/beta-docs/reference/api/crm/owners/v2)
* [Products](https://developers.hubspot.com/beta-docs/guides/api/crm/objects/products)
* [Properties](https://developers.hubspot.com/docs/api/crm/properties)
* [Tickets](https://developers.hubspot.com/docs/api/crm/tickets)
* [Workflows](https://developers.hubspot.com/docs/api-reference/legacy/create-manage-workflows-v3/get-automation-v3-workflows)

## Prerequisites

OAuth2 is used to authenticate the connector with HubSpot. A HubSpot account is required for the OAuth2 authentication process.

### Permissions and OAuth scopes

During the OAuth flow, HubSpot may present scopes that include permission to create, update, or delete data. This is due to how HubSpot groups permissions and exposes certain read APIs behind combined read/write scopes.

The HubSpot ( Real-Time ) connector uses these credentials only to read data from HubSpot and does not create, update, or delete CRM objects or other resources in your HubSpot account. All operations performed by this connector are read-only capture operations into Flow collections.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the HubSpot Real-Time connector.

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/capturePropertyHistory` | Capture Property History | Include historical data for changes to properties of HubSpot objects in captured documents. | boolean | Default: `false` |
| **`/credentials`** | Credentials | OAuth2 credentials | object | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required, `"OAuth Credentials"` |
| **`/credentials/client_id`** | OAuth Client ID | The OAuth app's client ID. | string | Required |
| **`/credentials/client_secret`** | OAuth Client Secret | The OAuth app's client secret. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | The refresh token received from the OAuth app. | string | Required |
| `/useLegacyNamingForCustomObjects` | Use Legacy Naming for Custom Objects | Controls how custom objects are named to avoid conflicts with standard HubSpot objects. When `false` (default), custom object bindings are prefixed with `custom_` (e.g., `custom_form_submissions`) to prevent naming collisions. When `true`, uses legacy behavior where custom objects can have the same name as standard objects, causing the custom object to shadow the standard object (e.g., a custom "form_submissions" object would replace the standard Form Submissions resource). This field is hidden in the dashboard and can only be edited via `flowctl`. Contact Estuary Support before changing this value, as it affects the names of discovered resources and collections. | boolean | Default: `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |
| `/schedule` | Calculated Property Refresh Schedule | The schedule for refreshing this binding's [calculated properties](#calculated-properties). Accepts a cron expression. For example, a schedule of `55 23 * * *` means the binding will refresh calculated properties at 23:55 UTC every day. If left empty, the binding will not refresh calculated properties. | string | 55 23 * * * |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-hubspot-native:dev
        config:
          capturePropertyHistory: false
          credentials:
            client_id: <secret>
            client_secret: <secret>
            credentials_title: OAuth Credentials
            refresh_token: <secret>
          useLegacyNamingForCustomObjects: false
    bindings:
      - resource:
          name: companies
          schedule: "55 23 * * *"
        target: ${PREFIX}/${COLLECTION_NAME}
```

## Calculated Properties

HubSpot CRM objects can contain [calculated properties](https://knowledge.hubspot.com/properties/create-calculation-properties), properties whose values are calculated at query time. Since calculated properties do not maintain state in HubSpot, calculated property updates do not update the associated record's `updatedAt` timestamp. The HubSpot connector uses the `updatedAt` timestamp to incrementally detect changes, and since calculated property updates don't update the `updatedAt` timestamp, calculated property updates are not incrementally captured by the connector.

To address this challenge, the HubSpot connector is able to refresh the values of calculated properties on a schedule after the initial backfill completes. This is controlled at a binding level by the cron expression in the [`schedule` property](#bindings). When a scheduled calculated property refresh occurs, the connector fetches every record's current calculated property values and merges them into the associated collection using [`merge` reduction strategies](/reference/reduction-strategies/merge).