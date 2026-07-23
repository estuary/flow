---
description: Use the Zuora connector to capture billing objects such as accounts, subscriptions, invoices, and payments into Estuary Flow. Authenticates with OAuth 2.0 client credentials and captures objects incrementally or as full-refresh snapshots.
---

# Zuora

This connector captures data from [Zuora](https://www.zuora.com/) objects into Estuary Flow collections.
It authenticates with Zuora's [OAuth 2.0](https://developer.zuora.com/api-references/api/overview/#section/Authentication) client credentials flow and reads data through Zuora's [AQuA API](https://docs.zuora.com/en/zuora-platform/data/aggregate-query-api-aqua/aqua-api-introduction).

## Supported data resources

The connector automatically discovers the Zuora objects available to your account and exposes each one as a binding. This includes standard billing objects such as `Account`, `Subscription`, `Invoice`, `Payment`, `Product`, and `RatePlan`, along with any custom objects your account exposes through the API.

Each discovered object is captured in one of two ways, chosen automatically based on the object's fields:

* **Incrementally**, for objects that have an `UpdatedDate` field. The connector uses `UpdatedDate` as a cursor to capture only new and changed records on each sync after the initial backfill.

* **As a full-refresh snapshot**, for objects that do not have an `UpdatedDate` field. Because these objects can't be read incrementally, the connector re-captures the object on each polling interval.

## Prerequisites

* A Zuora account with API access.

* An OAuth client (Client ID and Client Secret) created for a Zuora user. See [Authentication](#authentication) below to create one. The client inherits the permissions of the user it belongs to, so that user must have read access to every object you intend to capture.

* Your Zuora [data center's REST API base URL](https://developer.zuora.com/api-references/api/overview/#section/Introduction/Access-to-the-API) (for example, `https://rest.zuora.com` for US Production).

## Authentication

The connector authenticates using Zuora's OAuth 2.0 client credentials flow. You'll provide a **Client ID** and **Client Secret** generated for a Zuora user.

To create an OAuth client in Zuora:

1. Sign in to Zuora as an administrator.

2. Go to **Settings > Administration > Manage Users**.

3. Select the user the OAuth client should belong to. The connector will have the same data-access permissions as this user, so choose (or create) a user with read access to the objects you want to capture.

4. In the **OAuth Clients** section, enter a name for the client and click **Create**.

5. Zuora generates a **Client ID** and **Client Secret**. Copy both.

You'll use these as the `client_id` and `client_secret` values when configuring the connector.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Zuora source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authentication | OAuth 2.0 client credentials for your Zuora account. | object | Required |
| **`/credentials/client_id`** | Client ID | The Client ID of your Zuora OAuth client. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The Client Secret of your Zuora OAuth client. | string | Required |
| `/base_url` | Base URL | Zuora REST API base URL for your data center and environment. | string | `https://rest.zuora.com` |
| `/start_date` | Start Date | UTC date and time from which to start replicating data. Defaults to Zuora's founding year, January 1, 2007. | string | `2007-01-01T00:00:00Z` |

The `base_url` must be one of Zuora's [data center base URLs](https://developer.zuora.com/api-references/api/overview/#section/Introduction/Access-to-the-API).

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Name of the Zuora object to capture. | string | Required |
| `/interval` | Interval | Interval between data syncs for this resource. | string | PT5M |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-zuora:v1
        config:
          credentials:
            client_id: <secret>
            client_secret: <secret>
          base_url: https://rest.zuora.com
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: Account
          interval: PT5M
        target: ${PREFIX}/Account
      {...}
```
