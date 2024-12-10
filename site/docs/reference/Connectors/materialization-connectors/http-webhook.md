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
| **`/headers/customHeaders`**     | Headers        | Array of additional headers to include in the HTTP request. | object |               |

### Bindings

| Property           | Title          | Description                                    | Type   | Required/Default       |
|--------------------|----------------|------------------------------------------------|--------|------------------------|
| **`/relativePath`**| Relative Path  | The relative path on the server where data will be sent. | string | Required               |

## Sample

```yaml
bindings:
- source: ProductionData/orders/orderDetails
  resource:
    relativePath: webhook/estuary
endpoint:
  connector:
    image: ghcr.io/estuary/materialize-webhook:v1
    config:
      address: http://192.168.1.100:3000/
      headers:
        customHeaders:
          - name:  my-header
            value: my-value
          - name:  another-header
            value: another-value
```
