# Survey Monkey

This connector captures data from SurveyMonkey surveys into Flow collections via the SurveyMonkey API.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-surveymonkey:dev`](https://ghcr.io/estuary/source-surveymonkey:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

* [Surveys](https://developer.surveymonkey.com/api/v3/#api-endpoints-get-surveys)
* [Survey pages](https://developer.surveymonkey.com/api/v3/#api-endpoints-get-surveys-id-pages)
* [Survey questions](https://developer.surveymonkey.com/api/v3/#api-endpoints-get-surveys-survey_id-pages-page_id-questions)
* [Survey responses](https://developer.surveymonkey.com/api/v3/#api-endpoints-survey-responses)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

You'll need to configure a SurveyMonkey private app to integrate with Flow.

### Setup

1. Go to your your [SurveyMonkey apps page](https://developer.surveymonkey.com/apps) and create a new private app.
2. Set the following required [scopes](https://developer.surveymonkey.com/api/v3/#scopes):
   * View surveys
   * View responses
3. Deploy the app. This requires a paid SurveyMonkey plan; otherwise, [the app will be deleted in 90 days](https://developer.surveymonkey.com/api/v3/#deploying-an-app).

Once the app is set up, there are two ways to authenticate SurveyMonkey in Flow: using OAuth in the web app, or using an access token with the flowctl CLI.

#### OAuth authentication in the web app

You'll need the username and password of a SurveyMonkey user that is part of the [team](https://help.surveymonkey.com/en/billing/teams/)
for which the private app was created.

#### Manual authentication with flowctl

Note the client ID, secret, and access token for the private app you created. You'll use these in the connector configuration.

## Performance considerations

The SurveyMonkey API imposes [call limits](https://developer.surveymonkey.com/api/v3/#request-and-response-limits) of 500 per day
and 120 per minute.

This connector uses caching to avoid exceeding these limits.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SurveyMonkey source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#oauth-authentication-in-the-web-app),
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Credentials | Credentials for the service | object | Required |
| **`/credentials/access_token`** | Access Token | Access Token for your SurveyMonkey private app. | string | Required |
| **`/credentials/client_id`** | Client ID | Client ID associated with your SurveyMonkey private app. | string | Required |
| **`/credentials/client_secret`** | Client Secret | Client secret associated with your SurveyMonkey private app. | string | Required |
| **`/start_date`** | Start Date | UTC date and time in the format 2017-01-25T00:00:00Z. Any data before this date will not be replicated. | string | Required |
| `/survey_ids` | Survey Monkey survey IDs | IDs of the surveys from which you&#x27;d like to replicate data. If left empty, data from all boards to which you have access will be replicated. | array |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | SurveyMonkey resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-surveymonkey:dev
        config:
          credentials:
            access_token: {secret}
            client_id: XXXXXXXXXXXXXXXX
            client_secret: {secret}
          start_date: 2021-01-25T00:00:00Z
    bindings:
      - resource:
          stream: surveys
          syncMode: incremental
        target: ${PREFIX}/surveys
      {...}
```
