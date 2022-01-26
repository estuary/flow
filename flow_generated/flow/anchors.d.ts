// Ensure module has at least one export, even if otherwise empty.
export type __module = null;

// Generated from examples/stock-stats/schemas/exchange.schema.yaml#Exchange.
export type Exchange = /* Enum of market exchange codes. */ 'NASDAQ' | 'NYSE' | 'SEHK';

// Generated from examples/derive-patterns/schema.yaml#Int.
export type Int = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};

// Generated from examples/derive-patterns/schema.yaml#Join.
export type Join = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};

// Generated from examples/soak-tests/set-ops/schema.yaml#Output.
export type Output = /* Output merges expected and actual values for a given stream */ {
    appliedAdd?: number;
    appliedOps?: number;
    appliedRemove?: number;
    author: number;
    derived?: {
        [k: string]: {
            [k: string]: number;
        };
    };
    expectValues?: {
        [k: string]: number;
    };
    id: number;
    timestamp?: string;
};

// Generated from examples/stock-stats/schemas/L1-tick.schema.yaml#PriceAndSize.
export type PriceAndSize = {
    price: /* Dollar price. */ number;
    size: /* Number of shares. */ number;
};

// Generated from examples/stock-stats/schemas/daily-stat.schema.yaml#PriceStats.
export type PriceStats = {
    avgD?: /* Denominator of average. */ number;
    avgN?: /* Numerator of average. */ number;
    high?: number;
    low?: number;
};

// Generated from examples/stock-stats/schemas/L1-tick.schema.yaml#Security.
export type Security = /* Market security ticker name. */ string;

// Generated from examples/segment/event.schema.yaml#Segment.
export type Segment = {
    name: /* Name of the segment, scoped to the vendor ID. */ string;
    vendor: /* Vendor ID of the segment. */ number;
};

// Generated from examples/segment/derived.schema.yaml#SegmentDetail.
export type SegmentDetail = /* Status of a user's membership within a segment. */ {
    first?: /* Time at which this user was first added to this segment. */ string;
    last: /* Time at which this user was last updated within this segment. */ string;
    member: /* Is the user a current segment member? */ boolean;
    segment: Segment;
    value?: /* Most recent associated value. */ string;
};

// Generated from examples/segment/derived.schema.yaml#SegmentSet.
export type SegmentSet = SegmentDetail[];

// Generated from examples/derive-patterns/schema.yaml#String.
export type String = /* A document that holds a string */ {
    Key: string;
    String: string;
};
