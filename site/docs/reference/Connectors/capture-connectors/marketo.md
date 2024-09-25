
# Marketo

This connector captures data from Marketo into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-marketo:dev`](https://ghcr.io/estuary/source-marketo:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

This connector can be used to sync the following tables from Marketo:

* **activities\_X** where X is an activity type contains information about lead activities of the type X. For example, activities\_send\_email contains information about lead activities related to the activity type `send_email`. See the [Marketo docs](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getLeadActivitiesUsingGET) for a detailed explanation of what each column means.
* **activity\_types.** Contains metadata about activity types. See the [Marketo docs](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getAllActivityTypesUsingGET) for a detailed explanation of columns.
* **campaigns.** Contains info about your Marketo campaigns. [Marketo docs](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Campaigns/getCampaignsUsingGET).
* **leads.** Contains info about your Marketo leads. [Marketo docs](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Leads/getLeadByIdUsingGET).
* **lists.** Contains info about your Marketo static lists. [Marketo docs](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Static_Lists/getListByIdUsingGET).
* **programs.** Contains info about your Marketo programs. [Marketo docs](https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Programs/browseProgramsUsingGET).

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* \(Optional\) [allowlist the Estuary IP addresses](/reference/allow-ip-addresses) if needed
* An API-only Marketo User Role
* An Estuary Marketo API-only user
* A Marketo API Custom Service
* Marketo Client ID & Client Secret
* Marketo Base URL

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Marketo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/client_id` | Client ID | The Client ID of your Marketo developer application. | string | Required |
| `/client_secret` | Client Secret | The Client Secret of your Marketo developer application. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format 2021-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |
| `/domain_url` | Domain URL | Your Marketo Base URL. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Marketo project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-marketo:dev
        config:
          client_id: <secret>
          client_secret: <secret>
          start_date: 2017-01-25T00:00:00Z
          domain_url: <your domain URL>
    bindings:
      - resource:
          stream: leads
          syncMode: full_refresh
        target: ${PREFIX}/leads
      {...}
```
