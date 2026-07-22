---
description: Use the Smartsheet connector to capture sheets, reports, and their rows into Estuary Flow. Authenticates with a Smartsheet API access token and captures sheets incrementally and reports as full-refresh snapshots.
---

# Smartsheet

This connector captures data from [Smartsheet](https://www.smartsheet.com/) sheets and reports into Estuary Flow collections.
It authenticates with a Smartsheet [API access token](https://smartsheet.redoc.ly/#section/API-Basics/Authentication) and reads data through the [Smartsheet API 2.0](https://smartsheet.redoc.ly/).

## Supported data resources

The connector discovers every sheet and report available to your account and exposes them through four streams:

| Stream        | Description                                                                  | Replication           |
| ------------- | ---------------------------------------------------------------------------- | --------------------- |
| `sheets`      | Catalog metadata for each sheet.                                             | Incremental           |
| `sheet_rows`  | Individual rows from every sheet, each stamped with its parent `sheet_id`.   | Incremental           |
| `reports`     | Catalog metadata for each report.                                            | Full-refresh snapshot |
| `report_rows` | Individual rows from every report, each stamped with its parent `report_id`. | Full-refresh snapshot |

The connector chooses a replication mode per stream based on what the Smartsheet API can support:

- **Incrementally**, for `sheets` and `sheet_rows`. The connector uses the sheet list's `modifiedSince` filter and each sheet's `rowsModifiedSince` filter as cursors, capturing only new and changed records on each sync after the initial backfill.

:::warning
Because a row's `rowsModifiedSince` timestamp only advances when a cell is written, blank or never-populated rows are invisible to the incremental cursor. To close this gap, `sheet_rows` also runs a **daily full re-backfill** of every sheet (at midnight UTC) so those rows are reconciled at least once per day.
:::

- **As a full-refresh snapshot**, for `reports` and `report_rows`. The reports API's `modifiedSince` filter reflects when a report's _definition_ changed (a rename, a column change), not when the underlying sheet data it renders changed, and there is no row-level cursor. Because reports can't be read incrementally, the connector re-captures them on each polling interval.

## Prerequisites

- A Smartsheet account with access to the sheets and reports you want to capture.

- A Smartsheet **API access token**. See [Authentication](#authentication) below to generate one. The token inherits the permissions of the user who created it, so that user must have access to every sheet and report you intend to capture.

- The [Smartsheet region](#configuration) your account is hosted in (US, EU, Gov, or Australia).

## Authentication

The connector authenticates using a Smartsheet API access token, which acts as a bearer token on every request.

To generate a token:

1. Sign in to Smartsheet as the user whose access the connector should use. The connector will have the same access to sheets and reports as this user.
2. Go to **Account > Apps & Integrations > API Access**.
3. Click **Generate new access token** and give it a name.
4. Copy the generated token immediately — Smartsheet only displays it once.

You'll use this value as the `access_token` when configuring the connector.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Smartsheet source connector.

### Properties

#### Endpoint

| Property                        | Title            | Description                                                                                                                                        | Type   | Required/Default |
| ------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/credentials`**              | Authentication   | Smartsheet API access token credentials.                                                                                                           | object | Required         |
| **`/credentials/access_token`** | API Access Token | Smartsheet API Access Token, generated under Account > Apps & Integrations > API Access.                                                           | string | Required         |
| `/region`                       | Region           | The Smartsheet data center your account is hosted in. Determines the API base URL used to reach your account. One of `us`, `eu`, `gov`, or `au`.   | string | `us`             |
| `/start_date`                   | Start Date       | UTC date and time from which to start replicating data. Data generated before this date is not replicated. Defaults to 30 days before the present. | string | 30 days ago      |

The `region` selects which Smartsheet data center base URL the connector uses:

| Region | Base URL                            |
| ------ | ----------------------------------- |
| `us`   | `https://api.smartsheet.com/2.0`    |
| `eu`   | `https://api.smartsheet.eu/2.0`     |
| `gov`  | `https://api.smartsheetgov.com/2.0` |
| `au`   | `https://api.smartsheet.au/2.0`     |

#### Bindings

| Property    | Title    | Description                                                                            | Type   | Required/Default                |
| ----------- | -------- | -------------------------------------------------------------------------------------- | ------ | ------------------------------- |
| **`/name`** | Name     | Name of the resource to capture (`sheets`, `sheet_rows`, `reports`, or `report_rows`). | string | Required                        |
| `/interval` | Interval | Interval between data syncs for this resource.                                         | string | PT10M (sheets), PT30M (reports) |
| `/schedule` | Schedule | Cron schedule for the periodic full re-backfill (`sheet_rows` only).                   | string | `0 0 * * *`                     |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-smartsheet:v1
        config:
          credentials:
            credentials_title: API Access Token
            access_token: <secret>
          region: us
          start_date: "2024-01-01T00:00:00Z"
    bindings:
      - resource:
          name: sheets
          interval: PT10M
        target: ${PREFIX}/sheets
      - resource:
          name: sheet_rows
          interval: PT10M
          schedule: "0 0 * * *"
        target: ${PREFIX}/sheet_rows
      - resource:
          name: reports
          interval: PT30M
        target: ${PREFIX}/reports
      - resource:
          name: report_rows
          interval: PT30M
        target: ${PREFIX}/report_rows
```
