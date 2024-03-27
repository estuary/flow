# Criteo

This connector captures data from your Criteo account into Flow collections.

[`ghcr.io/estuary/source-criteo:dev`](https://ghcr.io/estuary/source-criteo:dev) provides the latest connector image. You can access past image versions by following the link in your browser.

## Supported Data Resources

The following data resources are supported through the Criteo APIs:

- [Analytics](https://developers.criteo.com/marketing-solutions/docs/analytics)
- [Audiences](https://developers.criteo.com/marketing-solutions/docs/audiences)
- [Campaigns](https://developers.criteo.com/marketing-solutions/docs/campaigns)
- [Creatives](https://developers.criteo.com/marketing-solutions/docs/creatives)


## Prerequisites

To use this connector, you will need the following:

- OAuth Client ID and Secret.

## Configuration

You can configure the Criteo source connector either through the Flow web app or by directly editing the Flow specification file. For more information on using this connector, see our guide on [connectors](../../../concepts/connectors.md#using-connectors). The values and specification sample below provide configuration details that are specific to the Criteo source connector.

### Properties

#### Endpoint

| Property            | Title          | Description                                                  | Type    | Required/Default       |
|---------------------|----------------|--------------------------------------------------------------|---------|------------------------|
| **`/advertiser_id`**| Advertiser ID  | Your unique Criteo Advertiser ID.                            | string  |               |
| **`/client_id`**    | OAuth Client ID| OAuth Client ID for accessing Criteo API.                    | string  | Required               |
| **`/client_secret`**| Client Secret  | OAuth Client Secret for accessing Criteo API.                | string  | Required               |
| **`/currency`**     | Currency       | Preferred currency for metrics.                              | string  |               |
| **`/dimensions`**   | Dimensions     | Specify dimensions if any, leave empty if not applicable.    |   |               |
| **`/metrics`**      | Metrics        | Specify metrics if any, leave empty if not applicable.       |   |               |
| **`/start_date`**   | Start Date     | Specify the start date for data extraction.                  | string  |               |

#### Bindings

| Property          | Title      | Description                    | Type    | Required/Default       |
| ----------------- | ---------- | ------------------------------ | ------- | ---------------------- |
| **`/stream`**     | Stream     | Resource of your Criteo project from which collections are captured. | string  | Required               |
| **`/syncMode`**   | Sync Mode  | Connection method.             | string  | Required               |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-criteo:dev
        config:
          advertiser_id: "sample_advertiser_id"
          client_id: "sample_api_key"
          client_secret: "sample_client_secret"
          currency: "USD"
          dimensions:
          metrics:
          start_date: 2022-01-21T00:00:00Z
    bindings:
      - resource:
          stream: transactions
          syncMode: incremental
        target: ${PREFIX}/transactions


