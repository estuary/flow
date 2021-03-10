import * as interfaces from './interfaces';

// Document is a relaxed signature for a Flow document of any kind.
export type Document = unknown;
// Lambda is a relaxed signature implemented by all Flow transformation lambdas.
export type Lambda = (source: Document, register?: Document, previous?: Document) => Document[];

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __interfaces_module = interfaces.__module;
// Import derivation classes from their implementation modules.
import {
    TestingIntStrings,
} from '../../examples/int-string.flow';

import {
    MarketingClicksWithViews,
    MarketingPurchaseWithOffers,
    MarketingViewsWithCampaign,
} from '../../examples/marketing/flow';

import {
    SoakSetOpsSets,
    SoakSetOpsSetsRegister,
} from '../../examples/soak-tests/set-ops/flow';

import {
    StockDailyStats,
} from '../../examples/stock-stats/flow';

// Build instances of each class, which will be bound to this module's router.
let __MarketingClicksWithViews: interfaces.MarketingClicksWithViews = new MarketingClicksWithViews();
let __MarketingPurchaseWithOffers: interfaces.MarketingPurchaseWithOffers = new MarketingPurchaseWithOffers();
let __MarketingViewsWithCampaign: interfaces.MarketingViewsWithCampaign = new MarketingViewsWithCampaign();
let __SoakSetOpsSets: interfaces.SoakSetOpsSets = new SoakSetOpsSets();
let __SoakSetOpsSetsRegister: interfaces.SoakSetOpsSetsRegister = new SoakSetOpsSetsRegister();
let __StockDailyStats: interfaces.StockDailyStats = new StockDailyStats();
let __TestingIntStrings: interfaces.TestingIntStrings = new TestingIntStrings();

// Now build the router that's used for transformation lambda dispatch.
let routes: { [path: string]: Lambda | undefined } = {
    '/derive/marketing/clicks-with-views/indexViews/Update': __MarketingClicksWithViews.indexViewsUpdate.bind(
        __MarketingClicksWithViews,
    ) as Lambda,
    '/derive/marketing/clicks-with-views/joinClickWithIndexedViews/Publish': __MarketingClicksWithViews.joinClickWithIndexedViewsPublish.bind(
        __MarketingClicksWithViews,
    ) as Lambda,
    '/derive/marketing/purchase-with-offers/indexClicks/Update': __MarketingPurchaseWithOffers.indexClicksUpdate.bind(
        __MarketingPurchaseWithOffers,
    ) as Lambda,
    '/derive/marketing/purchase-with-offers/indexViews/Update': __MarketingPurchaseWithOffers.indexViewsUpdate.bind(
        __MarketingPurchaseWithOffers,
    ) as Lambda,
    '/derive/marketing/purchase-with-offers/joinPurchaseWithViewsAndClicks/Publish': __MarketingPurchaseWithOffers.joinPurchaseWithViewsAndClicksPublish.bind(
        __MarketingPurchaseWithOffers,
    ) as Lambda,
    '/derive/marketing/views-with-campaign/indexCampaigns/Update': __MarketingViewsWithCampaign.indexCampaignsUpdate.bind(
        __MarketingViewsWithCampaign,
    ) as Lambda,
    '/derive/marketing/views-with-campaign/joinViewWithIndexedCampaign/Publish': __MarketingViewsWithCampaign.joinViewWithIndexedCampaignPublish.bind(
        __MarketingViewsWithCampaign,
    ) as Lambda,
    '/derive/soak/set-ops/sets/onOperation/Publish': __SoakSetOpsSets.onOperationPublish.bind(
        __SoakSetOpsSets,
    ) as Lambda,
    '/derive/soak/set-ops/sets-register/onOperation/Update': __SoakSetOpsSetsRegister.onOperationUpdate.bind(
        __SoakSetOpsSetsRegister,
    ) as Lambda,
    '/derive/soak/set-ops/sets-register/onOperation/Publish': __SoakSetOpsSetsRegister.onOperationPublish.bind(
        __SoakSetOpsSetsRegister,
    ) as Lambda,
    '/derive/stock/daily-stats/fromTicks/Publish': __StockDailyStats.fromTicksPublish.bind(
        __StockDailyStats,
    ) as Lambda,
    '/derive/testing/int-strings/appendStrings/Publish': __TestingIntStrings.appendStringsPublish.bind(
        __TestingIntStrings,
    ) as Lambda,
};

export { routes };
