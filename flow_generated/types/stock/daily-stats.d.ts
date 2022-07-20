// Generated from $anchor schema examples/stock-stats/schemas/L1-tick.schema.yaml#PriceAndSize."
export type PriceAndSize = {
    price: /* Dollar price. */ number;
    size: /* Number of shares. */ number;
};

// Generated from $anchor schema examples/stock-stats/schemas/L1-tick.schema.yaml#Security."
export type Security = /* Market security ticker name. */ string;

// Generated from $anchor schema examples/stock-stats/schemas/daily-stat.schema.yaml#PriceStats."
export type PriceStats = {
    avgD?: /* Denominator of average. */ number;
    avgN?: /* Numerator of average. */ number;
    high?: number;
    low?: number;
};

// Generated from $anchor schema examples/stock-stats/schemas/exchange.schema.yaml#Exchange."
export type Exchange = /* Enum of market exchange codes. */ 'NASDAQ' | 'NYSE' | 'SEHK';

// Generated from collection schema examples/stock-stats/schemas/daily-stat.schema.yaml.
// Referenced from examples/stock-stats/flow.yaml#/collections/stock~1daily-stats.
export type Document = /* Daily statistics of a market security. */ {
    ask?: PriceStats;
    bid?: PriceStats;
    date: string;
    exchange: Exchange;
    first?: /* First trade of the day. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    last?: /* Last trade of the day. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    price?: PriceStats;
    security: Security;
    spread?: PriceStats;
    volume?: /* Total number of shares transacted. */ number;
};

// Generated from derivation register schema examples/stock-stats/flow.yaml?ptr=/collections/stock~1daily-stats/derivation/register/schema.
// Referenced from examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation.
export type Register = unknown;

// Generated from transform fromTicks source schema examples/stock-stats/schemas/L1-tick.schema.yaml#/$defs/withRequired.
// Referenced from examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation/transform/fromTicks.
export type FromTicksSource = /* Level-one market tick of a security. */ {
    _meta?: Record<string, unknown>;
    ask: /* Lowest current offer to sell security. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    bid: /* Highest current offer to buy security. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    exchange: /* Enum of market exchange codes. */ 'NASDAQ' | 'NYSE' | 'SEHK';
    last: /* Completed transaction which generated this tick. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    security: /* Market security ticker name. */ string;
    time: string;
    [k: string]: Record<string, unknown> | boolean | string | null | undefined;
};

// Generated from derivation examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation.
// Required to be implemented by examples/stock-stats/flow.ts.
export interface IDerivation {
    fromTicksPublish(source: FromTicksSource, register: Register, previous: Register): Document[];
}
