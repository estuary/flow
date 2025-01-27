
# Pinterest
This connector captures data from Pinterest into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-pinterest:dev`](https://ghcr.io/estuary/source-pinterest:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites
To set up the Pinterest source connector, you'll need the following prerequisites:

* Pinterest App ID and secret key
* Refresh Token

## Setup
Follow the steps below to set up the Pinterest source connector.

1. Log into your Estuary Flow account.
2. Navigate to the "Captures" section
3. For the "Start Date," provide the date in YYYY-MM-DD format. Data added on and after this date will be replicated.
4. Next, go to "Authorization Method"
5. Authenticate your Pinterest account using OAuth2.0 or an Access Token. The OAuth2.0 authorization method is selected by default. For "Client ID" and "Client Secret," enter your Pinterest App ID and secret key. For the "Refresh Token," enter your Pinterest Refresh Token.
Click "Set up source."

## Configuration
You configure connectors either in the Flow web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Pinterest source connector.

### Properties

#### Endpoint
| Property      | Title      | Description                                                                                                                            | Type   | Required/Default |
| ------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| `/start_date` | Start Date | A date in the format YYYY-MM-DD. If you have not set a date, it would be defaulted to latest allowed date by api (89 days from today). | string | Required         |


#### Bindings

| Property        | Title     | Description                                                             | Type   | Required/Default |
| --------------- | --------- | ----------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your Pinterest project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                      | string | Required         |


### Sample

```json
{
  "required": ["start_date", "credentials"],
  "properties": {
    "start_date": {
      "pattern_descriptor": null
    },
    "credentials": {
      "discriminator": {
        "propertyName": "auth_method"
      },
      "oneOf": [
        {
          "title": "OAuth2.0",
          "type": "object",
          "x-oauth2-provider": "pinterest",
          "properties": {
            "auth_method": {
              "const": "oauth2.0",
              "order": 0,
              "type": "string",
              "default": "oauth2.0"
            },
            "client_id": {
              "airbyte_secret": true,
              "description": "The Client ID of your OAuth application",
              "title": "Client ID",
              "type": "string"
            },
            "client_secret": {
              "airbyte_secret": true,
              "description": "The Client Secret of your OAuth application.",
              "title": "Client Secret",
              "type": "string"
            },
            "refresh_token": {
              "airbyte_secret": true,
              "description": "Refresh Token to obtain new Access Token, when it's expired.",
              "title": "Refresh Token",
              "type": "string"
            }
          },
          "required": [
            "auth_method",
            "refresh_token"
          ]
        },
        {
          "title": "Access Token",
          "type": "object",
          "properties": {
            "access_token": {
              "airbyte_secret": true,
              "description": "The Access Token to make authenticated requests.",
              "title": "Access Token",
              "type": "string"
            },
            "auth_method": {
              "const": "access_token",
              "order": 0,
              "type": "string",
              "default": "access_token"
            }
          },
          "required": [
            "auth_method",
            "access_token"
          ]
        }
      ]
    }
  }
}
```

## Supported Streams
The Pinterest source connector supports the following streams:

* Account analytics (Incremental)
* Boards (Full table)
* Board sections (Full table)
* Pins on board section (Full table)
* Pins on board (Full table)
* Ad accounts (Full table)
* Ad account analytics (Incremental)
* Campaigns (Incremental)
* Campaign analytics (Incremental)
* Ad groups (Incremental)
* Ad group analytics (Incremental)
* Ads (Incremental)
* Ad analytics (Incremental)

## Performance Considerations
The Pinterest API imposes certain rate limits for the connector. Please take note of the following limits:

* Analytics streams: 300 calls per day per user
* Ad accounts streams (Campaigns, Ad groups, Ads): 1000 calls per minute per user per app
* Boards streams: 10 calls per second per user per app

:::note
For any additional information or troubleshooting, refer to the official [Pinterest API documentation](https://developers.pinterest.com/docs/overview/welcome/).
:::
