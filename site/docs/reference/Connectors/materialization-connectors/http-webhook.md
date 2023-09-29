# HTTP Webhook

This connector lets you materialize data from Estuary Flow directly to specified HTTP endpoints via webhooks.

[`ghcr.io/estuary/materialize-webhook:dev`](https://ghcr.io/estuary/materialize-webhook:dev) provides the latest connector image. For earlier versions, please follow the link in your browser.

## Prerequisites
To use this materialization connector, you’ll need the following:

- A server or service that can accept HTTP requests at the desired endpoint.
- The necessary authentication credentials. Authentication can be handled via `None`, `Basic`, or `OAuth`. For `Basic` authentication, you'll need a `username` and `password`. For `OAuth`, you'll need to provide `client_id` and `client_secret`.
- At least one Flow collection.

## Configuration
The Webhooks connector is available for use in the Flow web application. To learn more about connectors and setting them up, visit our guide on [using connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

## Properties

### Connection Details

| Property              | Title         | Description                                         | Type   | Required/Default       |
|-----------------------|---------------|-----------------------------------------------------|--------|------------------------|
| **`/endpointUrl`**    | Endpoint URL  | The URL of the endpoint to send data to.       | string | Required               |
| **`/authType`**       | Authentication| The type of authentication to use. | string | Required |
| **`/username`**       | Username      | Username for authentication.       | string |                |
| **`/password`**       | Password      | Password for authentication.       | string |                |
| **`/headers`**        | Headers       | Additional headers to include in the HTTP request. | object |                |

### Bindings

| Property              | Title          | Description                            | Type   | Required/Default       |
|-----------------------|----------------|----------------------------------------|--------|------------------------|
| **`/path`**    | Payload Path   | Path to extract the payload from the incoming data. | string | Required               |
| **`/method`**         | HTTP Method    | HTTP method to use (`GET`, `POST`, `PUT`, `DELETE`). | string | Required (default: `POST`) |

## Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-http-webhooks:dev
        config:
          endpointUrl: "http://webhook.endpoint.com"
          authType: "Basic"
          username: "user"
          password: "password"
    bindings:
      - resource:
          path: /data
          method: POST
        target: ${PREFIX}/webhook_target
```

## Timeout and Notifications

| Property              | Title         | Description                                         | Type   | Required/Default       |
|-----------------------|---------------|-----------------------------------------------------|--------|------------------------|
| **`/timeout`**        | Timeout       | Timeout for HTTP requests (in seconds).            | integer | |
| **`/notificationUrl`**| Notification URL | URL to send notifications on success/failure.   | string |               |



