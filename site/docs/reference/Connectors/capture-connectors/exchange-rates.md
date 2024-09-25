
# Exchange Rates API

This connector captures data from the [Exchange Rates API](https://exchangeratesapi.io/).
It creates a Flow collection with daily exchange rate data for a variety of supported currencies.
This simple connector is useful for educational and demonstration purposes.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-exchange-rates:dev`](https://ghcr.io/estuary/source-exchange-rates:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Prerequisites

* An API key generated through an [Exchange Rate API account](https://apilayer.com/marketplace/description/exchangerates_data-api?preview=true#pricing).
After you sign up, your API key can be found on your account page.
  * You may use the free account, but note that you'll be limited to the default base currency, EUR.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Exchange Rates source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/access_key`** | Access key | Your API access key. The key is case sensitive. | string | Required |
| `/base` | Base currency | ISO reference currency. See the [documentation](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html). Free plan doesn&#x27;t support Source Currency Switching, default base currency is EUR | string | EUR |
| `/ignore_weekends` | Ignore weekends | Ignore weekends? (Exchanges don&#x27;t run on weekends) | boolean | `true` |
| **`/start_date`** | Start date | The date in the format YYYY-MM-DD. Data will begin from this date. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Data stream from which Flow captures data. Always set to `exchange_rates`. | string | Required |
| **`/syncMode`** | Sync mode | Connection method. Always set to `incremental`. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-exchange-rates:dev
        config:
            base: EUR
            access_key: <secret>
            start_date: 2022-01-01
            ignore_weekends: true
    bindings:
      - resource:
           stream: exchange_rates
           syncMode: incremental
        target: ${PREFIX}/${COLLECTION_NAME}
```

This capture definition should only have one binding, as `exchange_rates` is the only available data stream.

[Learn more about capture definitions.](../../../concepts/captures.md#pull-captures)
