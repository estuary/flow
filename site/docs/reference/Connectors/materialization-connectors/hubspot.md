---
description: Materialize collections to HubSpot CRM objects such as Contacts, Companies, and Deals for reverse-ETL workflows.
---

# HubSpot

The HubSpot connector writes to HubSpot CRM objects such as Contacts,
Companies, and Deals.

## Prerequisites

- A HubSpot account.
- For each target object type, the HubSpot properties you intend to populate
  must already exist. It is recommended to create a dedicated property group to
  organize them.
- One property on each object type to use as the match property.

## Match Property

HubSpot record IDs are assigned automatically and cannot be set by the
connector. To match incoming documents to existing records, the connector
compares the HubSpot record against the **collection key**.

:::important
Using a **unique** property as the match key is strongly recommended.

Non-unique properties require a search to determine whether a record exists.
Due to HubSpot search API limitations, using a non-unique match property can
result in duplicate records.
:::

To determine if a property is unique, navigate in the HubSpot webapp to `Settings
/ Data Management / Properties` and check in the Property Rules if "require unique
values for this property" is enabled.  Alternatively, look at the [object
definition][] or use the [Properties API][] to list items with `hasUniqueValue`
set to `true`.

If there is no existing unique property that is suitable, it is recommended to
create a new property with "require unique values" set.

## Property name mapping

Collection field names are mapped to HubSpot property names automatically
similarly to how HubSpot generates property names: the field name is
lowercased, leading underscores are removed as well as other symbols
characters, and if the field begins with a number it will be prefixed with
`n`.Names are truncated to 100 characters.

## Deletions

Hard deletes are not supported. To track deletions, ensure your collection
documents include the `/_meta/op` field and create a corresponding `meta_op`
property in HubSpot.

Backfills do not remove existing HubSpot records.

## Enumerations

HubSpot enumeration properties require values from a pre-defined set. If a
document field contains a value not in the allowed set, the materialization
will return an error like:

```
HOWDY was not one of the allowed options: [IN_PROGRESS, NEW, UNQUALIFIED, ...]
```

Ensure your collection data (or a derivation that shapes it) only produces
values valid for enumeration properties.

## Configuration

You can configure connectors either in the Estuary web app or by directly
editing the Data Flow specification file. See [connectors][using-connectors] to
learn more.


### Properties

#### Endpoint

| Property                  | Title                      | Description                                          | Type                        | Required/Default |
| ------------------------- | -------------------------- | ---------------------------------------------------- | --------------------------- | ---------------- |
| **`/credentials`**        | Authentication             | Credentials for authenticating with HubSpot.         | [Credentials](#credentials) | Required         |
| `/advanced/limit`         | Request Limit              | Maximum API requests per second (excluding search).  | number                      | `10`             |
| `/advanced/burst`         | Request Burst Count        | Burst request allowance (excluding search).          | integer                     | `100`            |
| `/advanced/search_limit`  | Search Request Limit       | Maximum search API requests per second.              | number                      | `5`              |
| `/advanced/search_burst`  | Search Request Burst Count | Burst search request allowance.                      | integer                     | `5`              |

#### Credentials

For production tasks, use the `OAuth2` authentication method.

| Property                         | Title         | Description                                                         | Type   | Required/Default    |
| -------------------------------- | ------------- | ------------------------------------------------------------------- | ------ | ------------------- |
| **`/credentials/auth_type`**     | Auth Type     | Authentication method.                                              | string | Required: `OAuth2`  |
| `/credentials/refresh_token`     | Refresh Token | OAuth2 refresh token. Managed automatically by the Estuary web app. | string | Required           |

| Property                         | Title         | Description                                                        | Type   | Required/Default       |
| -------------------------------- | ------------- | ------------------------------------------------------------------ | ------ | ---------------------- |
| **`/credentials/auth_type`**     | Auth Type     | Authentication method.                                             | string | Required: `ServiceKey` |
| **`/credentials/service_key`**   | Service Key   | HubSpot private app access token with the required scope grants.   | string | Required               |

#### Bindings

| Property             | Title        | Description                                              | Type                         | Required/Default  |
| -------------------- | ------------ | -------------------------------------------------------- | ---------------------------- | ----------------- |
| **`/object`**        | Object type  | The HubSpot CRM object type to materialize into.         | [CRM Object](#crm-objects)   | Required          |
| `/delta_updates`     | Delta Update | Always `true` — this connector uses delta updates.       | boolean | `true` (read-only) |


#### CRM Objects

- Calls
- Companies
- Contacts
- Deals
- Emails
- Line Items
- Meetings
- Postal Mail
- Products
- Quotes
- Tasks
- Tickets

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-hubspot:v1
        config:
          credentials:
            auth_type: OAuth2
            refresh_token: <secret>
    bindings:
      - source: ${PREFIX}/contacts
        resource:
          object: Contacts
      - source: ${PREFIX}/companies
        resource:
          object: Companies
```


[object definition]: https://developers.hubspot.com/docs/api-reference/latest/crm/objects/contacts/object-definition
[Properties API]: https://developers.hubspot.com/docs/api-reference/latest/crm/properties/get-properties
[using-connectors]: /concepts/connectors.md#using-connectors
