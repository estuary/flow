import * as collections from './collections';
import * as registers from './registers';
import * as transforms from './transforms';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __collections_module = collections.__module;
export type __registers_module = registers.__module;
export type __transforms_module = transforms.__module;

// Generated from derivation examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation.
// Required to be implemented by examples/segment/flow.ts.
export interface ExamplesSegmentMemberships {
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        register: registers.ExamplesSegmentMemberships,
        previous: registers.ExamplesSegmentMemberships,
    ): collections.ExamplesSegmentMemberships[];
}

// Generated from derivation examples/segment/flow.yaml#/collections/examples~1segment~1profiles/derivation.
// Required to be implemented by examples/segment/flow.ts.
export interface ExamplesSegmentProfiles {
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        register: registers.ExamplesSegmentProfiles,
        previous: registers.ExamplesSegmentProfiles,
    ): collections.ExamplesSegmentProfiles[];
}

// Generated from derivation examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation.
// Required to be implemented by examples/segment/flow.ts.
export interface ExamplesSegmentToggles {
    fromSegmentationUpdate(
        source: collections.ExamplesSegmentEvents,
    ): registers.ExamplesSegmentToggles[];
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        register: registers.ExamplesSegmentToggles,
        previous: registers.ExamplesSegmentToggles,
    ): collections.ExamplesSegmentToggles[];
}

// Generated from derivation examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation.
// Required to be implemented by examples/marketing/flow.ts.
export interface MarketingClicksWithViews {
    indexViewsUpdate(
        source: collections.MarketingViewsWithCampaign,
    ): registers.MarketingClicksWithViews[];
    joinClickWithIndexedViewsPublish(
        source: collections.MarketingOfferClicks,
        register: registers.MarketingClicksWithViews,
        previous: registers.MarketingClicksWithViews,
    ): collections.MarketingClicksWithViews[];
}

// Generated from derivation examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation.
// Required to be implemented by examples/marketing/flow.ts.
export interface MarketingPurchaseWithOffers {
    indexClicksUpdate(
        source: collections.MarketingClicksWithViews,
    ): registers.MarketingPurchaseWithOffers[];
    indexViewsUpdate(
        source: collections.MarketingViewsWithCampaign,
    ): registers.MarketingPurchaseWithOffers[];
    joinPurchaseWithViewsAndClicksPublish(
        source: collections.MarketingPurchases,
        register: registers.MarketingPurchaseWithOffers,
        previous: registers.MarketingPurchaseWithOffers,
    ): collections.MarketingPurchaseWithOffers[];
}

// Generated from derivation examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation.
// Required to be implemented by examples/marketing/flow.ts.
export interface MarketingViewsWithCampaign {
    indexCampaignsUpdate(
        source: collections.MarketingCampaigns,
    ): registers.MarketingViewsWithCampaign[];
    joinViewWithIndexedCampaignPublish(
        source: collections.MarketingOfferViews,
        register: registers.MarketingViewsWithCampaign,
        previous: registers.MarketingViewsWithCampaign,
    ): collections.MarketingViewsWithCampaign[];
}

// Generated from derivation examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets/derivation.
// Required to be implemented by examples/soak-tests/set-ops/flow.ts.
export interface SoakSetOpsSets {
    onOperationPublish(
        source: collections.SoakSetOpsOperations,
        register: registers.SoakSetOpsSets,
        previous: registers.SoakSetOpsSets,
    ): collections.SoakSetOpsSets[];
}

// Generated from derivation examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets-register/derivation.
// Required to be implemented by examples/soak-tests/set-ops/flow.ts.
export interface SoakSetOpsSetsRegister {
    onOperationUpdate(
        source: collections.SoakSetOpsOperations,
    ): registers.SoakSetOpsSetsRegister[];
    onOperationPublish(
        source: collections.SoakSetOpsOperations,
        register: registers.SoakSetOpsSetsRegister,
        previous: registers.SoakSetOpsSetsRegister,
    ): collections.SoakSetOpsSetsRegister[];
}

// Generated from derivation examples/stock-stats/flow.yaml#/collections/stock~1daily-stats/derivation.
// Required to be implemented by examples/stock-stats/flow.ts.
export interface StockDailyStats {
    fromTicksPublish(
        source: transforms.StockDailyStatsfromTicksSource,
        register: registers.StockDailyStats,
        previous: registers.StockDailyStats,
    ): collections.StockDailyStats[];
}

// Generated from derivation examples/temp-sensors/flow.yaml#/collections/temperature~1average-by-location/derivation.
// Required to be implemented by examples/temp-sensors/flow.ts.
export interface TemperatureAverageByLocation {
    readingsUpdate(
        source: collections.TemperatureReadings,
    ): registers.TemperatureAverageByLocation[];
    readingsPublish(
        source: collections.TemperatureReadings,
        register: registers.TemperatureAverageByLocation,
        previous: registers.TemperatureAverageByLocation,
    ): collections.TemperatureAverageByLocation[];
    sensorsUpdate(
        source: collections.TemperatureSensors,
    ): registers.TemperatureAverageByLocation[];
    sensorsPublish(
        source: collections.TemperatureSensors,
        register: registers.TemperatureAverageByLocation,
        previous: registers.TemperatureAverageByLocation,
    ): collections.TemperatureAverageByLocation[];
}

// Generated from derivation examples/int-string.flow.yaml#/collections/testing~1int-strings/derivation.
// Required to be implemented by examples/int-string.flow.ts.
export interface TestingIntStrings {
    appendStringsPublish(
        source: collections.TestingIntString,
        register: registers.TestingIntStrings,
        previous: registers.TestingIntStrings,
    ): collections.TestingIntStrings[];
}
