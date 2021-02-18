// Ensure module has at least one export, even if otherwise empty.
export type __module = null;

// Generated from examples/stock-stats/schemas/exchange.schema.yaml.
export type Exchange = /* Enum of market exchange codes. */ "NASDAQ" | "NYSE" | "SEHK";

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/header.
export type Header = /* Common properties of generated operations */ {
    Author: number;
    ID: number;
    Ones: number;
    Op: number;
    Type: string;
};

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/mutateOp.
export type MutateOp = /* Operation which mutates a stream */ {
    Author: number;
    ID: number;
    Ones: number;
    Op: number;
    Type: "add" | "remove";
    Values: {
        [k: string]: number;
    };
};

// Generated from examples/stock-stats/schemas/L1-tick.schema.yaml#/$defs/priceAndSize.
export type PriceAndSize = {
    price: /* Dollar price. */ number;
    size: /* Number of shares. */ number;
};

// Generated from examples/stock-stats/schemas/daily-stat.schema.yaml#/$defs/priceStats.
export type PriceStats = {
    avgD?: /* Denominator of average. */ number;
    avgN?: /* Numerator of average. */ number;
    high?: number;
    low?: number;
};

// Generated from examples/stock-stats/schemas/L1-tick.schema.yaml#/$defs/security.
export type Security = /* Market security ticker name. */ string;

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/verifyOp.
export type VerifyOp = /* Operation which verifies the expected value of a stream */ {
    Author: number;
    ID: number;
    Ones: number;
    Op: number;
    TotalAdd: number;
    TotalRemove: number;
    Type: "verify";
    Values: {
        [k: string]: number;
    };
};
