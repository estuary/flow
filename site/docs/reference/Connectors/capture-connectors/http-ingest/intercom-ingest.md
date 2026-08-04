---
description: Capture Intercom webhook events with the HTTP Ingest connector. Configure webhook paths, capture query parameters, and set authentication details.
slug: /reference/Connectors/capture-connectors/intercom-ingest/
---

# Intercom HTTP Ingest (Webhook)

The Intercom HTTP Ingest connector allows you to capture data from _incoming_ HTTP requests from Intercom.
A common use case is to capture webhook deliveries, turning them into an Estuary collection.

## Usage

### Configure an Intercom webhook

1. To begin, use the dashboard to create a capture. Once published, the connector overview displays
a unique URL for your public endpoint.

2. Navigate to your App in your Developer Hub and select the `Webhooks` from the configuration options

3. Under `Endpoint URL` enter in the unique URL generated for your Estuary Webhook endpoint in the format `https://<your-webhook-url>/webhook-data`

4. Configure the `Topics` section to trigger on your preferred webhook events and click save. Optionally, you can select `Send a test request` to preview how the data would be ingested into Estuary.

### Webhook URLs

To determine the full URL, start with the base URL from the Estuary web app (for example `https://abc123-8080.us-central1.v1.estuary-data.dev`), and then append the path.

The path will be whatever is in the `paths` endpoint configuration field (`/webhook-data` by default). For example, your full webhook URL would be `https://<your-unique-hostname>/webhook-data`. You can add additional paths to `paths`, and the connector will accept webhook requests on each of them. Each path will correspond to a separate binding. If you're editing the capture via the UI, click the "re-fresh" button after editing the URL paths in the endpoint config to see the resulting collections in the bindings editor. For example, if you set the path to `/my-webhook.json`, then the full URL for that binding would be `https://<your-unique-hostname>/my-webhook.json`.

Any URL query parameters that are sent on the request will be captured and serialized under `/_meta/query/*` in the documents. For example, a webhook request that's sent to `/webhook-data?testKey=testValue` would result in a document like:

```
{
  "_meta": {
    "webhookId": "...",
    "query": {
      "testKey": "testValue"
    },
    ...
  }
  ...
}
```

### Authentication

The connector can optionally require each request to present an authentication token as part of an `Authorization: Bearer ` HTTP header. To enable authentication, generate a secret and paste it into the "Require Auth Token" field. We recommend using a password manager to generate these values, but keep in mind that not all systems will be able to send values with certain special characters, so you may want to disable special characters when you generate the secret. If you enable authentication, then each incoming request must have an `Authorization` header with the value of your token. For example, if you use an auth token value of `mySecretToken`, then the header on each request must be `Authorization: Bearer mySecretToken`.

**If you don't enable authentication, then anyone who knows the URL will be able to publish data to your collection.** We recommend using authentication whenever possible.

### Webhook signature verification

Intercom webhooks use HMAC-SHA1 signatures. This verification scheme is not yet supported by this connector. If this is a requirement for your use case, please contact [`support@estuary.dev`](mailto://support@estuary.dev) and let us know.

## Configuration

### Endpoint properties

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/requireAuthToken` |  | Optional bearer token to authenticate webhook requests. WARNING: If this is empty or unset, then anyone who knows the URL of the connector will be able to write data to your collections. | null, string | `null` |
| `/paths` | URL Paths |  List of URL paths to accept requests at. Discovery will return a separate collection for each given path. Paths must be provided without any percent encoding, and should not include any query parameters or fragment. | null, string | `null` |
| `/allowedCorsOrigins` | CORS Allowed Origins | List of allowed CORS origins. Set to an empty array to disable CORS. Must not include `*` when an authentication token is configured. | string array | `["*"]` |
| `/signatureConfig` | Signature Verification | Configuration for verifying webhook signatures. | object | `{"provider": "none"}` |

### Resource properties

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/idFromHeader` |  | Set the &#x2F;&#x5F;meta&#x2F;webhookId from the given HTTP header in each request. If not set, then a random id will be generated automatically. If set, then each request will be required to have the header, and the header value will be used as the value of &#x60;&#x2F;&#x5F;meta&#x2F;webhookId&#x60;. | null, string |  |
| `/path` |  | The URL path to use for adding documents to this binding. Defaults to the name of the collection. | null, string |  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-intercom-ingest:v1
        config:
          paths:
            - /webhook-data
          signatureConfig:
            provider: none
    bindings:
      - resource:
          path: /webhook-data
          stream: /webhook-data
        target: ${PREFIX}/${COLLECTION_NAME}
```
