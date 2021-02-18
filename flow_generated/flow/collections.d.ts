import * as anchors from './anchors';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __anchors_module = anchors.__module;

// Generated from examples/marketing/schema.yaml#/$defs/campaign.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1campaigns.
export type MarketingCampaigns = /* Configuration of a marketing campaign. */ {
    campaign_id: number;
};

// Generated from examples/marketing/schema.yaml#/$defs/click-with-view.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views.
export type MarketingClicksWithViews = /* Click event joined with it's view. */ {
    click_id: string;
    timestamp: string;
    user_id: string;
    view: {
        campaign: {
            campaign_id: number;
        } | null;
        campaign_id: number;
        timestamp: string;
        user_id: string;
        view_id: string;
    } | null;
    view_id: string;
};

// Generated from examples/marketing/schema.yaml#/$defs/click.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1offer~1clicks.
export type MarketingOfferClicks = /* Event which captures a user's click of a marketing offer. */ {
    click_id: string;
    timestamp: string;
    user_id: string;
    view_id: string;
};

// Generated from examples/marketing/schema.yaml#/$defs/view.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1offer~1views.
export type MarketingOfferViews = /* Event which captures a user's view of a marketing offer. */ {
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
};

// Generated from examples/marketing/flow.yaml?ptr=/collections/marketing~1purchase-with-offers/schema.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers.
export type MarketingPurchaseWithOffers = /* Purchase event joined with prior offer views and clicks. */ {
    clicks: /* Click event joined with it's view. */ {
        click_id: string;
        timestamp: string;
        user_id: string;
        view: {
            campaign: {
                campaign_id: number;
            } | null;
            campaign_id: number;
            timestamp: string;
            user_id: string;
            view_id: string;
        } | null;
        view_id: string;
    }[];
    purchase_id: number;
    user_id: string;
    views: /* View event joined with it's campaign. */ {
        campaign: {
            campaign_id: number;
        } | null;
        campaign_id: number;
        timestamp: string;
        user_id: string;
        view_id: string;
    }[];
};

// Generated from examples/marketing/schema.yaml#/$defs/purchase.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1purchases.
export type MarketingPurchases = /* Event which captures a user's purchase of a product. */ {
    purchase_id: number;
    user_id: string;
};

// Generated from examples/marketing/schema.yaml#/$defs/view-with-campaign.
// Referenced as schema of examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign.
export type MarketingViewsWithCampaign = /* View event joined with it's campaign. */ {
    campaign: {
        campaign_id: number;
    } | null;
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
};

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/operation.
// Referenced as schema of examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1operations.
export type SoakSetOpsOperations = /* Union type over MutateOp and VerifyOp */ {
    Author: number;
    ID: number;
    Ones: number;
    Op: number;
    Type: "add" | "remove" | "verify";
    Values: {
        [k: string]: number;
    };
};

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/outputWithReductions.
// Referenced as schema of examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets.
export type SoakSetOpsSets = /* Output merges expected and actual values for a given stream */ {
    AppliedAdd?: number;
    AppliedOps?: number[];
    AppliedRemove?: number;
    Author: number;
    Derived?: {
        [k: string]: {
            [k: string]: number;
        };
    };
    ExpectAdd?: number;
    ExpectRemove?: number;
    ExpectValues?: {
        [k: string]: number;
    };
    ID: number;
};

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/output.
// Referenced as schema of examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets-register.
export type SoakSetOpsSetsRegister = /* Output merges expected and actual values for a given stream */ {
    AppliedAdd?: number;
    AppliedOps?: number[];
    AppliedRemove?: number;
    Author: number;
    Derived?: {
        [k: string]: {
            [k: string]: number;
        };
    };
    ExpectAdd?: number;
    ExpectRemove?: number;
    ExpectValues?: {
        [k: string]: number;
    };
    ID: number;
};

// Generated from examples/stock-stats/schemas/daily-stat.schema.yaml.
// Referenced as schema of examples/stock-stats/flow.yaml#/collections/stock~1daily-stats.
export type StockDailyStats = /* Daily statistics of a market security. */ {
    ask?: /* Low, high, and average ask price. */ anchors.PriceStats;
    bid?: /* Low, high, and average bid price. */ anchors.PriceStats;
    date: string;
    exchange: /* Enum of market exchange codes. */ anchors.Exchange;
    first?: /* First trade of the day. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    last?: /* Last trade of the day. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    price?: /* Low, high, and average transaction price (weighted by shares). */ anchors.PriceStats;
    security: /* Market security ticker name. */ anchors.Security;
    spread?: /* Low, high, and average spread of bid vs ask. */ anchors.PriceStats;
    volume?: /* Total number of shares transacted. */ number;
};

// Generated from examples/stock-stats/schemas/L1-tick.schema.yaml.
// Referenced as schema of examples/stock-stats/flow.yaml#/collections/stock~1ticks.
export type StockTicks = /* Level-one market tick of a security. */ {
    _meta?: Record<string, unknown>;
    ask?: /* Lowest current offer to sell security. */ anchors.PriceAndSize;
    bid?: /* Highest current offer to buy security. */ anchors.PriceAndSize;
    exchange: /* Enum of market exchange codes. */ anchors.Exchange;
    last?: /* Completed transaction which generated this tick. */ anchors.PriceAndSize;
    security: /* Market security ticker name. */ anchors.Security;
    time: string;
    [k: string]: Record<string, unknown> | boolean | string | null | undefined;
};

// Generated from examples/int-string-flow.yaml?ptr=/collections/testing~1int-string/schema.
// Referenced as schema of examples/int-string-flow.yaml#/collections/testing~1int-string.
export type TestingIntString = {
    i: number;
    s: string;
};

// Generated from examples/int-string-flow.yaml?ptr=/collections/testing~1int-strings/schema.
// Referenced as schema of examples/int-string-flow.yaml#/collections/testing~1int-strings.
export type TestingIntStrings = {
    i: number;
    s?: string[];
};

// Generated from examples/weird-types-flow.yaml?ptr=/collections/weird-types~1optional-multi-types/schema.
// Referenced as schema of examples/weird-types-flow.yaml#/collections/weird-types~1optional-multi-types.
export type WeirdTypesOptionalMultiTypes = {
    any?: unknown;
    boolOrArrayOrNull?: unknown[] | boolean | null;
    boolOrString?: boolean | string;
    intDifferentRanges?: number;
    intOrNum?: number;
    intOrNumOrNull?: number | null;
    intOrNumOverlappingRanges?: number;
    intOrObjectOrNull?: Record<string, unknown> | number | null;
    stringOrInt?: number | string;
    theKey: string;
};

// Generated from examples/weird-types-flow.yaml?ptr=/collections/weird-types~1optionals/schema.
// Referenced as schema of examples/weird-types-flow.yaml#/collections/weird-types~1optionals.
export type WeirdTypesOptionals = {
    array?: unknown[];
    bool?: boolean;
    int?: number;
    number?: number;
    object?: Record<string, unknown>;
    string?: string;
    theKey: string;
};

// Generated from examples/weird-types-flow.yaml?ptr=/collections/weird-types~1required-nullable/schema.
// Referenced as schema of examples/weird-types-flow.yaml#/collections/weird-types~1required-nullable.
export type WeirdTypesRequiredNullable = {
    array: unknown[] | null;
    boolean: boolean | null;
    integer: number | null;
    null: null;
    number: number | null;
    object: Record<string, unknown> | null;
    string: string | null;
    theKey: string;
};
