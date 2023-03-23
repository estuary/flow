import { IDerivation, Document, SourceFromTicks } from 'flow/stock/daily-stats.ts';
import moment from "https://deno.land/x/momentjs@2.29.1-deno/mod.ts";

// Implementation for derivation stock/daily-stats.
export class Derivation extends IDerivation {
    fromTicks(source: { doc: SourceFromTicks }): Document[] {
        const tick = source.doc;
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
