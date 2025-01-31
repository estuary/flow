
# Criteo

This connector captures data from [Criteo's API](https://developers.criteo.com/marketing-solutions/reference).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-criteo:dev`](https://ghcr.io/estuary/source-criteo:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Criteo APIs:

* [Ad Sets](https://developers.criteo.com/marketing-solutions/docs/ad-set)
* [Advertisers](https://developers.criteo.com/marketing-solutions/docs/get-advertiser-portfolio)
* [Audiences](https://developers.criteo.com/marketing-solutions/docs/audiences)
* [Audiences (Legacy)](https://developers.criteo.com/marketing-solutions/v2020.07/docs/get-existing-audiences)
* [Campaigns (Legacy)](https://developers.criteo.com/marketing-solutions/v2020.07/docs/get-existing-campaigns)
* [Campaigns (Preview)](https://developers.criteo.com/marketing-solutions/docs/campaigns)
* [Categories (Legacy)](https://developers.criteo.com/marketing-solutions/v2020.07/docs/campaigns-get-campaign-categories)

You may also configure multiple [Report](https://developers.criteo.com/marketing-solutions/docs/campaign-statistics) resources based on desired dimensions and metrics.

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To set up a Criteo source connector in Flow, you will need:

* A Criteo Client ID
* A Criteo Client Secret
* One or more Advertiser IDs

See Criteo's documentation for information on [authentication](https://developers.criteo.com/marketing-solutions/docs/authentication) and where to find your IDs.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Criteo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/client_id` | Client ID | The Criteo client ID used for authentication. | string | Required |
| `/client_secret` | Client Secret | The Criteo client secret used for authentication. | string | Required |
| `/advertiser_ids` | Advertiser IDs | One or more Criteo advertiser IDs. | string[] | Required |
| `/start_date` | Start Date | Earliest date to read data from. Uses UTC date-time format, ex. `YYYY-MM-DDT00:00:00.000Z`. | string | Required |
| `/reports` | Reports | Optional configuration for additional report streams. | object[] |  |
| `/reports/-/name` | Report Name | The report's name. | string |  |
| `/reports/-/dimensions` | Report Dimensions | An array of dimensions. See [Criteo's documentation](https://developers.criteo.com/marketing-solutions/docs/campaign-statistics#dimensions) for possible options. | string |  |
| `/reports/-/metrics` | Report Metrics | An array of metrics. See [Criteo's documentation](https://developers.criteo.com/marketing-solutions/docs/campaign-statistics#full-list-of-metrics) for possible options. | string |  |
| `/reports/-/currency` | Report Currency | The report's currency. | string | `USD` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Criteo resource from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-criteo:dev
        config:
          client_id: {secret}
          client_secret: {secret}
          advertiser_ids:
            - "12345"
            - "67890"
          start_date: 2025-01-01T00:00:00.000Z
    bindings:
      - resource:
          stream: advertisers
          syncMode: full_refresh
        target: ${PREFIX}/advertisers
      {...}
```
