
# HTTP Ingest (Webhook)

The HTTP Ingest connector allows you to capture data from _incoming_ HTTP requests.
A common use case is to capture webhook deliveries, turning them into a Flow collection.

If you need to capture a dataset hosted at at HTTP endpoint, see the [HTTP File](./http-file.md) connector.

The connector is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-http-ingest:dev`](https://ghcr.io/estuary/source-http-ingest:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Usage

This connector is different from most other capture connectors in that it's not designed to pull data from a specific
system or endpoint. It requires no endpoint-specific configuration, and can accept any and all valid JSON objects from any source.

This is especially useful if you want to test out Flow or see how your webhook data will come over.

To begin, use the web app to create a capture. Once published, the confirmation dialog displays
a unique URL for your public endpoint. By default, this will accept webhook requests at `https://<your-public-endpoint>/webhook-data`, but you can customize the path, or even capture from multiple URL paths if you like.

You're now ready to send data to Flow.

### Send sample data to Flow

1. After publishing the capture, click the endpoint link from the confirmation dialog to open the Swagger UI page for your capture.

2. Expand **POST** or **PUT** and click **Try it out** to send some example JSON documents using the UI. You can also copy the provided `curl` commands to send data via the command line.

3. After sending data, go to the Collections page of the Flow web app and find the collection associated with your capture.
Click **Details** to view the data preview.

### Webhook URLs

To configure a webhook in another service, such as Github, Shopify, or Segment, you'll need to paste a webhook URL into the configuration of their service.

To determine the full URL, start with the base URL from the Flow web app (for example `https://abc123-8080.us-central1.v1.estuary-data.dev`), and then append the path.

The path will be whatever is in the `paths` endpoint configuration field (`/webhook-data` by default). For example, your full webhook URL would be `https://<your-unique-hostname>/webhook-data`. You can add additional paths to `paths`, and the connector will accept webhook requests on each of them. Each path will correspond to a separate binding. If you're editing the capture via the UI, click the "re-fresh" button after editing the URL paths in the endpoint config to see the resulting collections in the bindings editor. For example, if you set the path to `/my-webhook.json`, then the full URL for that binding would be `https://<your-unique-hostname>/my-webhook.json`.

Any URL query parameters that are sent on the request will be captured and serialized under `/_meta/query/*` the in documents. For example, a webhook request that's sent to `/webhook-data?testKey=testValue` would result in a document like:

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

### Path parameters

Paths are allowed to contain parameter placeholders, which will be captured and serialized under `/_meta/pathParams/*` in the documents. For example, if you configure a path for `/foo/{fooId}` a webhook request that's sent to `/foo/123` would result in a document like:

```
{
  "_meta": {
    "webhookId": "...",
    "pathParams": {
      "fooId": "123"
    },
    "reqPath": "/foo/{fooId}",
    ...
  }
  ...
}
```

Multiple parameters are allowed, for example `/foo/{fooId}/bar/{barId}`. Each parameter corresponds to exactly one path segment in the request URL. Capturing multiple segments in a single parameter is not supported. The syntax and semantics of the path specification follow the [OpenAPI specification](https://swagger.io/docs/specification/v3_0/paths-and-operations/#path-templating) (a.k.a Swagger).

Path parameters are automatically added to the collection write schema as required properties, so they can be used as part of the collection key by editing the collection during capture creation.

Care must be taken when specifying multiple paths, to ensure they don't conflict with each other. For example, you may not specify both `/{paramA}` and `/{paramB}`, because it would be impossible to determine which path to use for a request to `/123`.

### Webhook IDs

Webhook delivery is typically "at least once". This means that webhooks from common services such as Github, Segment, Shopify, etc. may sometimes be sent multiple times.
In order to prevent problems due to duplicate processing of webhooks, these services typically provide either an HTTP header or a field within each document that serves
as a unique ID for each webhook event. This can be used to deduplicate the events in your `webhook-data` collection. The key of the discovered `webhook-data` collection is `/_meta/webhookId`.
By default, this value is generated automatically by the connector, and no-deduplication will be performed.
You can set the `idFromHeader` option in the [resource configuration](#resource-configuration) to have the connector automatically assign the value of the given HTTP header to the `/_meta/webhookId` property.
Doing so means that a materialization of the `webhook-data` collection will automatically deduplicate the webhook events.

Here's a table with some common webhook services and headers that they use:

| Service | Value to use for `idFromHeader`  |
|---------|----------------------------------|
| Github  | `X-Github-Event`                 |
| Shopify | `X-Shopify-Webhook-Id`           |
| Zendesk | `x-zendesk-webhook-id`           |
| Jira    | `X-Atlassian-Webhook-Identifier` |

### Custom collection IDs

Some webhooks don't pass a deduplication ID as part of the HTTP headers. That's fine, and you can still easily deduplicate the events.
To do so, you'll just need to customize the `schema` and `key` of your webhook-data collection, or bind the webhook to an existing collection that already has the correct `schema` and `key`.
Just set the `key` to the field(s) within the webhook payload that uniquely identify the event.
For example, to capture webhooks from Segment, you'll want to set the `key` to `["/messageId"]`, and ensure that the `schema` requires that property to exist and be a `string`.

### Authentication

The connector can optionally require each request to present an authentication token as part of an `Authorization: Bearer ` HTTP header. To enable authentication, generate a secret and paste it into the "Require Auth Token" field. We recommend using a password manager to generate these values, but keep in mind that not all systems will be able to send values with certain special characters, so you may want to disable special characters when you generate the secret. If you enable authentication, then each incoming request must have an `Authorization` header with the value of your token. For example, if you use an auth token value of `mySecretToken`, then the header on each request must be `Authorization: Bearer mySecretToken`.

**If you don't enable authentication, then anyone who knows the URL will be able to publish data to your collection.** We recommend using authentication whenever possible.

### Webhook signature verification

This connector does not yet support webhook signature verification. If this is a requirement for your use case, please contact [`support@estuary.dev`](mailto://support@estuary.dev) and let us know.

## Endpoint Configuration

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **** | EndpointConfig |  | object | Required |
| `/require_auth_token` | Authentication token | Optional bearer token to authenticate webhook requests. WARNING: If this is empty or unset, then anyone who knows the URL of the connector will be able to write data to your collections. | null, string | `null` |
| `/paths` | URL Paths |  List of URL paths to accept requests at. Discovery will return a separate collection for each given path. Paths must be provided without any percent encoding, and should not include any query parameters or fragment. | null, string | `null` |

List of URL paths to accept requests at. Discovery will return a separate collection for each given path. Paths must be provided without any percent encoding, and should not include any query parameters or fragment.
## Resource configuration

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **** | ResourceConfig |  | object | Required |
| `/idFromHeader` |  | Set the &#x2F;&#x5F;meta&#x2F;webhookId from the given HTTP header in each request. If not set, then a random id will be generated automatically. If set, then each request will be required to have the header, and the header value will be used as the value of &#x60;&#x2F;&#x5F;meta&#x2F;webhookId&#x60;. | null, string |  |
| `/path` |  | The URL path to use for adding documents to this binding. Defaults to the name of the collection. | null, string |  |
| `/stream` |  | The name of the binding, which is used as a merge key when doing Discovers. | null, string |  |
