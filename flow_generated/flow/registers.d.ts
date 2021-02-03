import * as anchors from './anchors';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __anchors_module = anchors.__module;

// Generated from examples/marketing/flow.yaml?ptr=/collections/marketing~1clicks-with-views/derivation/register/schema.
// Referenced as register_schema of examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation.
export type MarketingClicksWithViews = {
    campaign: {
        campaign_id: number;
    } | null;
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
} | null;

// Generated from examples/marketing/flow.yaml?ptr=/collections/marketing~1purchase-with-offers/derivation/register/schema.
// Referenced as register_schema of examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation.
export type MarketingPurchaseWithOffers = {
    clicks: {
        [k: string]: /* Click event joined with it's view. */ {
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
    };
    lastSeen?: string;
    views: {
        [k: string]: /* View event joined with it's campaign. */ {
            campaign: {
                campaign_id: number;
            } | null;
            campaign_id: number;
            timestamp: string;
            user_id: string;
            view_id: string;
        };
    };
};

// Generated from examples/marketing/flow.yaml?ptr=/collections/marketing~1views-with-campaign/derivation/register/schema.
// Referenced as register_schema of examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation.
export type MarketingViewsWithCampaign = {
    campaign_id: number;
} | null;

// Generated from examples/stock-stats/flow.yaml?ptr=/collections/stock~1daily-stats/derivation/register/schema.
// Referenced as register_schema of examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation.
export type StockDailyStats = unknown;

// Generated from examples/int-string-flow.yaml?ptr=/collections/testing~1int-strings/derivation/register/schema.
// Referenced as register_schema of examples/int-string-flow.yaml#/collections/testing~1int-strings/derivation.
export type TestingIntStrings = unknown;

// Generated from examples/soak-tests/set-ops/flow.yaml?ptr=/collections/soak~1set-ops~1sets/derivation/register/schema.
// Referenced as register_schema of examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets/derivation.
export type SoakSetOpsSets = unknown;

// Generated from examples/soak-tests/set-ops/schema.yaml#/$defs/outputWithReductions.
// Referenced as register_schema of examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets-register/derivation.
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
