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
        target: ${PREFIX}/${COLLECTION_NAME}
```
