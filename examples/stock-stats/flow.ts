import { IDerivation, Document, Register, FromTicksSource } from 'flow/stock/daily-stats';

import * as moment from 'moment';

// Implementation for derivation examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation.
export class Derivation implements IDerivation {
    fromTicksPublish(tick: FromTicksSource, _register: Register, _previous: Register): Document[] {
        // Current bid/ask price spread of the tick.
        const spread = tick.ask.price - tick.bid.price;
        // Truncate full UTC timestamp to current date.
        const date = moment.utc(tick.time).format('YYYY-MM-DD');

        return [
            {
                exchange: tick.exchange,
                security: tick.security,
                date: date,
                // Price stat uses a by-volume weighted average of trades.
                price: {
                    low: tick.last.price,
                    high: tick.last.price,
                    avgN: tick.last.price * tick.last.size,
                    avgD: tick.last.size,
                },
                // Bid, ask, and spread stats use equal weighting of observed prices across ticks.
                bid: {
                    low: tick.bid.price,
                    high: tick.bid.price,
                    avgN: tick.bid.price,
                    avgD: 1,
                },
                ask: {
                    low: tick.ask.price,
                    high: tick.ask.price,
                    avgN: tick.ask.price,
                    avgD: 1,
                },
                spread: { low: spread, high: spread, avgN: spread, avgD: 1 },
                volume: tick.last.size,
                first: tick.last,
                last: tick.last,
            },
        ];
    }
}
