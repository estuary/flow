// Generated from $anchor schema examples/stock-stats/schemas/L1-tick.schema.yaml#PriceAndSize."
export type PriceAndSize = {
    price: /* Dollar price. */ number;
    size: /* Number of shares. */ number;
};

// Generated from $anchor schema examples/stock-stats/schemas/L1-tick.schema.yaml#Security."
export type Security = /* Market security ticker name. */ string;

// Generated from $anchor schema examples/stock-stats/schemas/exchange.schema.yaml#Exchange."
export type Exchange = /* Enum of market exchange codes. */ 'NASDAQ' | 'NYSE' | 'SEHK';

// Generated from collection schema examples/stock-stats/schemas/L1-tick.schema.yaml.
// Referenced from examples/stock-stats/flow.yaml#/collections/stock~1ticks.
export type Document = /* Level-one market tick of a security. */ {
    _meta?: Record<string, unknown>;
    ask?: PriceAndSize;
    bid?: PriceAndSize;
    exchange: Exchange;
    last?: PriceAndSize;
    security: Security;
    time: string;
    [k: string]: Record<string, unknown> | boolean | string | null | undefined;
};
