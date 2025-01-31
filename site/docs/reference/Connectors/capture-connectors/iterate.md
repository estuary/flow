
# Iterate

This connector captures data from Iterate into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-iterate:dev`](https://ghcr.io/estuary/source-iterate:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Iterate API:

* [Surveys](https://iterate.docs.apiary.io/#reference/0//surveys)
* [Survey Responses](https://iterate.docs.apiary.io/#reference/0//surveys/{id}/responses)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* An Iterate [API access token](https://iterate.docs.apiary.io/#introduction/overview/authentication)

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Iterate source connector.

### Properties

#### Endpoint

The properties in the table below reflect manual authentication using the CLI. In the Flow web app,
you'll sign in directly and won't need the access token.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | Access Token | Iterate Access token. | string | Required |
| **`/credentials/credentials_title`** | Credentials | Name of the credentials set | string | Required, `"Private App Credentials"` |


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
        image: ghcr.io/estuary/source-iterate:dev
        config:
            credentials:
                credentials_title: Private App Credentials
                access_token: <secret>
    bindings:
      - resource:
          name: surveys
        target: ${PREFIX}/admins
      - resource:
          name: survey_responses
        target: ${PREFIX}/companies
```
