# HTTP Webhook

This connector lets you materialize data from Estuary Flow directly to specified HTTP endpoints via webhooks.

[`ghcr.io/estuary/materialize-webhook:v1`](https://ghcr.io/estuary/materialize-webhook:v1) provides the latest connector image. For earlier versions, please follow the link in your browser.

## Prerequisites
To use this materialization connector, you’ll need the following:

- A server or service that can accept HTTP requests at the target endpoint.
- At least one Flow collection.

## Configuration
The Webhooks connector is available for use in the Flow web application. To learn more about connectors and setting them up, visit our guide on [using connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

## Properties

### Endpoint

| Property           | Title          | Description                              | Type   | Required/Default       |
|--------------------|----------------|------------------------------------------|--------|------------------------|
| **`/address`**     | Address        | The URL of the endpoint to send data to. | string | Required               |
| **`/method`**      | HTTP Method    | HTTP method to use (e.g., `POST` or `PUT`). | string | default: `POST` |
| **`/headers`**     | Headers        | Additional headers to include in the HTTP request. | object |               |

### Bindings

| Property           | Title          | Description                                    | Type   | Required/Default       |
|--------------------|----------------|------------------------------------------------|--------|------------------------|
| **`/relativePath`**| Relative Path  | The relative path on the server where data will be sent. | string | Required               |

## Sample

```yaml
bindings:
- source: TestSamagra/attendance-1/attendance
  resource:
    relativePath: webhook/estuary
endpoint:
  connector:
    image: ghcr.io/estuary/materialize-webhook:v1
    config:
      address: http://64.227.184.175:3000/
      method: POST
      headers:
        Content-Type: application/json
        Authorization: Bearer <your_token>
```
```
