
# Aircall

This connector captures data from Aircall into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-aircall:dev`](https://ghcr.io/estuary/source-aircall:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
To set up the Aircall connector, you need the following prerequisite:

- Access Token: An access token acting as a bearer token is required for the connector to work. You can find the access token in the settings of [Aircall](https://dashboard.aircall.io/integrations/api-keys).

## Setup
Follow the steps below to set up the Aircall connector.

1. Obtain an Aircall access token from the Aircall [settings](https://dashboard.aircall.io/integrations/api-keys).

### Set up the Aircall connector in Estuary Flow
1. Log into your Estuary Flow account.
2. In the left navigation bar, click on "Captures". In the top-left corner, click "Connector Search".
3. Enter the name for the Aircall connector and select "Aircall" from the dropdown.
4. Fill out the following endpoint configurations:
   - `api_id`: The auto-generated ID.
   - `api_token`: The access token obtained from Aircall settings.
   - `start_date`: Date filter for eligible streams. Enter the desired start date.

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Aircall source connector.

### Properties

#### Endpoint
| Property      | Title      | Description                                                                       | Type   | Required/Default |
| ------------- | ---------- | --------------------------------------------------------------------------------- | ------ | ---------------- |
| `/api_id`     | API ID     | App ID found at [settings](https://dashboard.aircall.io/integrations/api-keys)    | string | Required         |
| `/api_token`  | API Token  | App token found at [settings](https://dashboard.aircall.io/integrations/api-keys) | string | Required         |
| `/start_date` | Start Date | Date time filter for incremental filter, Specify which date to extract from.      | string | Required         |

#### Bindings

| Property        | Title     | Description                                                           | Type   | Required/Default |
| --------------- | --------- | --------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your Aircall project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                    | string | Required         |


### Sample

```json
{
  "properties": {
    "start_date": {
      "default": "2023-01-01T00:00:00.000Z",
      "format": null
    }
  }
}
```

## Supported Streams
The Aircall connector supports the following streams:

- calls
- company
- contacts
- numbers
- tags
- user_availablity
- users
- teams
- webhooks

## API Method Example
An example of an API method call for Aircall:

`GET https://api.aircall.io/v1/numbers`


## Performance Considerations
The Aircall API currently uses v1. The connector defaults to using v1.
