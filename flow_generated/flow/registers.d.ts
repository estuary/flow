import * as anchors from './anchors';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __anchors_module = anchors.__module;

// Generated from examples/acmeBank.flow.yaml?ptr=/collections/acmeBank~1balances/derivation/register/schema.
// Referenced as register_schema of examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export type AcmeBankBalances = unknown;

// Generated from examples/citi-bike/idle-bikes.flow.yaml?ptr=/collections/examples~1citi-bike~1idle-bikes/derivation/register/schema.
// Referenced as register_schema of examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation.
export type ExamplesCitiBikeIdleBikes = string | null;

// Generated from examples/citi-bike/last-seen.flow.yaml?ptr=/collections/examples~1citi-bike~1last-seen/derivation/register/schema.
// Referenced as register_schema of examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen/derivation.
export type ExamplesCitiBikeLastSeen = unknown;

// Generated from examples/citi-bike/ride.schema.yaml#/$defs/terminus.
// Referenced as register_schema of examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations/derivation.
export type ExamplesCitiBikeRidesAndRelocations = /* Station and time at which a trip began or ended */ {
    station: /* A Citi Bike Station */ {
        geo?: /* Location of this station */ /* Geographic Location as Latitude & Longitude */ {
            latitude: number;
            longitude: number;
        };
        id: /* Unique identifier for this station */ number;
        name: /* Human-friendly name of this station */ string;
    };
    timestamp: /* Timestamp as YYYY-MM-DD HH:MM:SS.F in UTC */ string;
};

// Generated from examples/citi-bike/stations.flow.yaml?ptr=/collections/examples~1citi-bike~1stations/derivation/register/schema.
// Referenced as register_schema of examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations/derivation.
export type ExamplesCitiBikeStations = unknown;

// Generated from examples/net-trace/services.flow.yaml?ptr=/collections/examples~1net-trace~1services/derivation/register/schema.
// Referenced as register_schema of examples/net-trace/services.flow.yaml#/collections/examples~1net-trace~1services/derivation.
export type ExamplesNetTraceServices = unknown;

// Generated from examples/re-key/schema.yaml#/$defs/join_register.
// Referenced as register_schema of examples/re-key/flow.yaml#/collections/examples~1re-key~1stable_events/derivation.
export type ExamplesReKeyStableEvents = /* Register that's keyed on anonymous ID, which:
  1) Stores anonymous events prior to a stable ID being known, and thereafter
  2) Stores a mapped stable ID for this anonymous ID.
 */ {
    events: /* An interesting event, keyed on an anonymous ID */
    | {
              anonymous_id: string;
              event_id: string;
          }[]
        | null;
    stable_id?: string;
};

// Generated from examples/segment/flow.yaml?ptr=/collections/examples~1segment~1memberships/derivation/register/schema.
// Referenced as register_schema of examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation.
export type ExamplesSegmentMemberships = unknown;

// Generated from examples/segment/flow.yaml?ptr=/collections/examples~1segment~1profiles/derivation/register/schema.
// Referenced as register_schema of examples/segment/flow.yaml#/collections/examples~1segment~1profiles/derivation.
export type ExamplesSegmentProfiles = unknown;

// Generated from examples/segment/flow.yaml?ptr=/collections/examples~1segment~1toggles/derivation/register/schema.
// Referenced as register_schema of examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation.
export type ExamplesSegmentToggles = {
    event?: /* A segment event adds or removes a user into a segment. */ {
        event: /* V4 UUID of the event. */ string;
        remove?: /* User is removed from the segment. */ /* May be unset or "true", but not "false" */ true;
        segment: {
            name: /* Name of the segment, scoped to the vendor ID. */ string;
            vendor: /* Vendor ID of the segment. */ number;
        };
        timestamp: /* RFC 3339 timestamp of the segmentation. */ string;
        user: /* User ID. */ string;
        value?: /* Associated value of the segmentation. */ string;
    };
    firstAdd?: true;
};

// Generated from examples/shopping/cart-updates-with-products.flow.yaml?ptr=/collections/examples~1shopping~1cartUpdatesWithProducts/derivation/register/schema.
// Referenced as register_schema of examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation.
export type ExamplesShoppingCartUpdatesWithProducts = {
    id: number;
    name: string;
    price: number;
} | null;

// Generated from examples/shopping/carts.flow.yaml?ptr=/collections/examples~1shopping~1carts/derivation/register/schema.
// Referenced as register_schema of examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation.
export type ExamplesShoppingCarts = {
    cartItems: {
        [k: string]: /* Represents a (possibly 0) quantity of a product within the cart */ {
            product?: /* A product that is available for purchase */ {
                id: number;
                name: string;
                price: number;
            };
            quantity?: number;
        }[];
    };
    userId: number;
};

// Generated from examples/shopping/cart.schema.yaml.
// Referenced as register_schema of examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation.
export type ExamplesShoppingPurchases = /* Roll up of all products that users have added to a pending purchase */ {
    items: /* Represents a (possibly 0) quantity of a product within the cart */ {
        product?: /* A product that is available for purchase */ {
            id: number;
            name: string;
            price: number;
        };
        quantity?: number;
    }[];
    userId: number;
};

// Generated from examples/source-schema/flow.yaml?ptr=/collections/examples~1source-schema~1restrictive/derivation/register/schema.
// Referenced as register_schema of examples/source-schema/flow.yaml#/collections/examples~1source-schema~1restrictive/derivation.
export type ExamplesSourceSchemaRestrictive = unknown;

// Generated from examples/wiki/pages.flow.yaml?ptr=/collections/examples~1wiki~1pages/derivation/register/schema.
// Referenced as register_schema of examples/wiki/pages.flow.yaml#/collections/examples~1wiki~1pages/derivation.
export type ExamplesWikiPages = unknown;

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

// Generated from examples/derive-patterns/schema.yaml#Join.
// Referenced as register_schema of examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation.
export type PatternsInnerJoin = anchors.Join;

// Generated from examples/derive-patterns/schema.yaml#Join.
// Referenced as register_schema of examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation.
export type PatternsOneSidedJoin = anchors.Join;

// Generated from examples/derive-patterns/join-outer.flow.yaml?ptr=/collections/patterns~1outer-join/derivation/register/schema.
// Referenced as register_schema of examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation.
export type PatternsOuterJoin = unknown;

// Generated from examples/derive-patterns/summer.flow.yaml?ptr=/collections/patterns~1sums-db/derivation/register/schema.
// Referenced as register_schema of examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-db/derivation.
export type PatternsSumsDb = unknown;

// Generated from examples/derive-patterns/summer.flow.yaml?ptr=/collections/patterns~1sums-register/derivation/register/schema.
// Referenced as register_schema of examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register/derivation.
export type PatternsSumsRegister = number;

// Generated from examples/derive-patterns/zero-crossing.flow.yaml?ptr=/collections/patterns~1zero-crossing/derivation/register/schema.
// Referenced as register_schema of examples/derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing/derivation.
export type PatternsZeroCrossing = number;

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

// Generated from examples/stock-stats/flow.yaml?ptr=/collections/stock~1daily-stats/derivation/register/schema.
// Referenced as register_schema of examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation.
export type StockDailyStats = unknown;

// Generated from examples/temp-sensors/schemas.yaml#/$defs/tempToLocationRegister.
// Referenced as register_schema of examples/temp-sensors/flow.yaml#/collections/temperature~1averageByLocation/derivation.
export type TemperatureAverageByLocation = {
    avgC?: number;
    lastReading?: /* Timestamp of the most recent reading for this named location */ string;
    location?: /* GeoJSON Point */ /* The precise geographic location of the sensor */ {
        bbox?: number[];
        coordinates: number[];
        type: 'Point';
    };
    locationName?: string | null;
    maxTempC?: number;
    minTempC?: number;
    numReadings?: number;
    totalC?: number;
};

// Generated from examples/temp-sensors/flow.yaml?ptr=/collections/temperature~1averageTemps/derivation/register/schema.
// Referenced as register_schema of examples/temp-sensors/flow.yaml#/collections/temperature~1averageTemps/derivation.
export type TemperatureAverageTemps = unknown;
