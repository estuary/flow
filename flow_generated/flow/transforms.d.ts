import * as anchors from './anchors';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __anchors_module = anchors.__module;

// Generated from examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/derivation/transform/fromPermissive/source/schema.
// Referenced as source schema of transform examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation/transform/fromPermissive.
export type ExamplesSourceSchemaRestrictivefromPermissiveSource = /* Require that the documents from permissive all have these fields */ {
    a: number;
    b: boolean;
    c: number;
    id: string;
};

// Generated from examples/stock-stats/schemas/L1-tick.schema.yaml#/$defs/withRequired.
// Referenced as source schema of transform examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation/transform/fromTicks.
export type StockDailyStatsfromTicksSource = /* Level-one market tick of a security. */ {
    _meta?: Record<string, unknown>;
    ask: /* Lowest current offer to sell security. */ anchors.PriceAndSize;
    bid: /* Highest current offer to buy security. */ anchors.PriceAndSize;
    exchange: /* Enum of market exchange codes. */ anchors.Exchange;
    last: /* Completed transaction which generated this tick. */ anchors.PriceAndSize;
    security: /* Market security ticker name. */ anchors.Security;
    time: string;
    [k: string]: Record<string, unknown> | boolean | string | null | undefined;
};
