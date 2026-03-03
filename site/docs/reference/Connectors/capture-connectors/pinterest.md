
# Pinterest
This connector captures data from Pinterest into Estuary collections.

## Prerequisites
To set up the Pinterest source connector, you'll need the following prerequisites:

* Pinterest App ID and secret key
* Refresh Token

## Setup
Follow the steps below to set up the Pinterest source connector.

1. Log into your Estuary account.
2. Navigate to the "Captures" section
3. For the "Start Date," provide the date in YYYY-MM-DD format. Data added on and after this date will be replicated.
4. Next, go to "Authorization Method"
5. Authenticate your Pinterest account using OAuth2.0 or an Access Token. The OAuth2.0 authorization method is selected by default. For "Client ID" and "Client Secret," enter your Pinterest App ID and secret key. For the "Refresh Token," enter your Pinterest Refresh Token.
Click "Set up source."

## Configuration
You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Pinterest source connector.

### Properties

#### Endpoint
| Property      | Title      | Description                                                                                                                            | Type   | Required/Default |
| ------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| `/start_date` | Start Date | A date in the format YYYY-MM-DD. If you have not set a date, it would be defaulted to latest allowed date by api (89 days from today). | string | Required         |
| `/credentials` | Credentials | An authorization method to authenticate your Pinterest account. | object | Required |
| `/credentials/auth_method` | Authentication Method | One of `oauth2.0` or `access_token`. | string | Required |
| `/credentials/client_id` | Client ID | Client ID for the `oauth2.0` auth method. | string | Required for `oauth2.0` |
| `/credentials/client_secret` | Client Secret | Client secret for the `oauth2.0` auth method. | string | Required for `oauth2.0` |
| `/credentials/refresh_token` | Refresh Token | Refresh token for the `oauth2.0` auth method. | string | Required for `oauth2.0` |
| `/credentials/access_token` | Access Token | Access token for the `access_token` auth method. | string | Required for `access_token` auth |

#### Bindings

| Property        | Title     | Description                                                             | Type   | Required/Default |
| --------------- | --------- | ----------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your Pinterest project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                      | string | Required         |


### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-pinterest:v1
        config:
          start_date: 2026-01-01
          credentials:
            auth_method: access_token
            access_token: <secret>
    bindings:
      - resource:
          name: boards
          syncMode: full_refresh
        target: ${PREFIX}/${COLLECTION_NAME}
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
