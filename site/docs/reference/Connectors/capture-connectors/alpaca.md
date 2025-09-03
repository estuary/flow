
# Alpaca

This connector captures stock trade data from the [Alpaca Market Data API](https://alpaca.markets/docs/market-data/) into a Flow collection.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-alpaca:dev`](https://ghcr.io/estuary/source-alpaca:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Real-time and historical trade data

The Alpaca Market Data API comprises multiple APIs for stock trades, including
the [Trades REST API](https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/historical/) for historical trade data
and [websocket streaming via the Data API](https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/) for real-time trade data.

Historical trade data is available from the Alpaca Market Data API starting 01-01-2016. As such, the
connector configuration requires a start date for the backfill to be on or after 01-01-2016.

This connector uses both APIs to capture historical and real-time data in parallel.
It uses the Trades API to perform a historical backfill starting from the start date you specify and stopping when it reaches the present.
At the same time, the connector uses websocket streaming to initiate a real-time stream of trade data starting at the present moment and continuing indefinitely until you stop the capture process.

As a result, you'll get data from a historical time period you specify, as well as the lowest-latency
possible updates of new trade data, but there will be some overlap in the two data streams.
See [limitations](#limitations) to learn more about reconciling historical and real-time data.

## Supported data resources

Alpaca supports over 8000 stocks and ETFs. You simply supply a list of [symbols](https://eoddata.com/symbols.aspx) to Flow when you configure the connector.
To check whether Alpaca supports a symbol, you can use the [Alpaca Broker API](https://alpaca.markets/docs/api-references/broker-api/assets/#retrieving-an-asset-by-symbol).

You can use this connector to capture data from up to 20 stock symbols into Flow collections in a single capture
(to add more than 20, set up multiple captures).
For a given capture, data from all symbols is captured to a single collection.

## Prerequisites

To use this connector, you'll need:

* An Alpaca account.
    * To access complete stock data in real-time, you'll need the [Unlimited plan](https://alpaca.markets/docs/market-data/#subscription-plans).
      To access a smaller sample of trade data with a 15-minute delay, you can use a Free plan, making sure to set **Feed** to `iex` and choose the **Free Plan** option when [configuring the connector](#endpoint).

* Your Alpaca [API Key ID and Secret Key](https://alpaca.markets/docs/market-data/getting-started/#creating-an-alpaca-account-and-finding-your-api-keys).

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Alpaca source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/advanced` | Advanced Options | Options for advanced users. You should not typically need to modify these. | object |  |
| `/advanced/disable_backfill` | Disable Historical Data Backfill | Disables historical data backfill via the historical data API. Data will only be collected via streaming. | boolean |  |
| `/advanced/disable_real_time` | Disable Real-Time Streaming | Disables real-time streaming via the websocket API. Data will only be collected via the backfill mechanism. | boolean |  |
| `/advanced/is_free_plan` | Free Plan | Set this if you are using a free plan. Delays data by 15 minutes. | boolean |  |
| `/advanced/max_backfill_interval` | Maximum Backfill Interval | The largest time interval that will be requested for backfills. Using smaller intervals may be useful when tracking many symbols. Must be a valid Go duration string. | string |  |
| `/advanced/min_backfill_interval` | Minimum Backfill Interval | The smallest time interval that will be requested for backfills after the initial backfill is complete. Must be a valid Go duration string. | string |  |
| `/advanced/stop_date` | Stop Date | Stop backfilling historical data at this date. | string |  |
| **`/api_key_id`** | Alpaca API Key ID | Your Alpaca API key ID. | string | Required |
| **`/api_secret_key`** | Alpaca API Secret Key | Your Alpaca API Secret key. | string | Required |
| **`/feed`** | Feed | The feed to pull market data from. [Choose from `iex` or `sip`](https://alpaca.markets/docs/market-data/#subscription-plans); set `iex` if using a free plan.| string | Required |
| **`/start_date`** | Start Date | Get trades starting at this date. Has no effect if changed after the capture has started. Must be no earlier than 2016-01-01T00:00:00Z.| string | Required |
| **`/symbols`** | Symbols | Comma separated list of symbols to monitor. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Unique name for this binding. Cannot be changed once set. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
      endpoint:
        connector:
          image: "ghcr.io/estuary/source-alpaca:dev"
          config:
            api_key_id: <SECRET>
            api_secret_key: <SECRET>
            feed: iex
            start_date: 2022-11-01T00:00:00Z
            symbols: AAPL,MSFT,AMZN,TSLA,GOOGL,GOOG,NVDA,BRK.B,META,UNH
            advanced:
              is_free_plan: true
      bindings:
        - resource:
            name: trades
          target: ${PREFIX}/${CAPTURE_NAME}/trades
```

## Limitations

#### Capturing data for more than 20 symbols in a single capture could result in API errors.

If you need to capture data for more than 20 symbols, we recommend splitting them between two captures.
Support for a larger number of symbols in a single capture is planned for a future release.

#### Separate historical and real-time data streams will result in some duplicate trade documents.

As discussed [above](#real-time-and-historical-trade-data), the connector captures historical and real-time data in two different streams.
As the historical data stream catches up to the present, it will overlap with the beginning of the real-time data stream, resulting in some duplicated documents.
These will have [identical properties from Alpaca](https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/historical/#response-object-properties), but different [metadata from Flow](../../../concepts/collections.md#documents).

There are several ways to resolve this:

* If you plan to materialize to an endpoint for which standard (non-delta) updates are supported, Flow will resolve the duplicates during the materialization process.
Unless otherwise specified in their [documentation page](../materialization-connectors/README.md), materialization connectors run in standard updates mode.
If a connector supports both modes, it will default to standard updates.

* If you plan to materialize to an endpoint for which [delta updates](/concepts/materialization/#delta-updates) is the only option,
ensure that the endpoint system supports the equivalent of [lastWriteWins](../../reduction-strategies/firstwritewins-and-lastwritewins.md) reductions.
