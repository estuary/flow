---
description: Capture Brevo contact data with Estuary's connector, including attributes, lists, folders, segments, senders, and webhooks, using API key authentication.
---

# Brevo

This connector captures data from [Brevo's REST API](https://developers.brevo.com/reference) into Estuary collections.

## Supported data resources

| Resource | Replication | API reference |
|---|---|---|
| `contacts` | Incremental | [Get all the contacts](https://developers.brevo.com/reference/get-contacts) |
| `contacts_attributes` | Snapshot | [List all attributes](https://developers.brevo.com/reference/getattributes-1) |
| `contacts_lists` | Snapshot | [Get all the lists](https://developers.brevo.com/reference/getlists-1) |
| `contacts_folders` | Snapshot | [Get all folders](https://developers.brevo.com/reference/getfolders-1) |
| `contacts_segments` | Snapshot | [Get all the segments](https://developers.brevo.com/reference/getsegments) |
| `senders` | Snapshot | [Get email senders](https://developers.brevo.com/reference/getsenders-1) |
| `webhooks` | Snapshot | [Get all webhooks](https://developers.brevo.com/reference/getwebhooks-1) |

By default, each resource is mapped to an Estuary collection through a separate binding.

If your use case requires additional Brevo APIs, such as campaigns, transactional
email activity, or CRM objects, [contact us](mailto:info@estuary.dev) to discuss
expanding this connector.

## Prerequisites

You will need a Brevo API key. See [Brevo's documentation](https://developers.brevo.com/docs/getting-started#using-your-api-key-to-authenticate) for instructions on creating one.

Note that Brevo applies [per-endpoint rate limits](https://developers.brevo.com/docs/api-limits). The
`/contacts` family allows 36,000 requests per hour, but most other endpoints —
including those behind the `senders` and `webhooks` bindings — fall into a
100-requests-per-hour bucket on standard accounts. At their default intervals
those two bindings together use about four requests an hour, so headroom is
ample. Raise their intervals if you run other integrations against the same key.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Brevo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Credentials Title | Name of the authentication method. Must be `API Key`. | string | Required |
| **`/credentials/access_token`** | API Key | The Brevo API key used for authentication. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Brevo resource from which collections are captured. | string | Required |
| `/interval` | Interval | Interval between updates for this resource. | string | Varies by resource |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-brevo:v2
        config:
          credentials:
            credentials_title: API Key
            access_token: {secret}
    bindings:
      - resource:
          name: contacts
          interval: PT5M
        target: ${PREFIX}/contacts
      - resource:
          name: contacts_lists
          interval: PT1H
        target: ${PREFIX}/contacts_lists
      {...}
```
