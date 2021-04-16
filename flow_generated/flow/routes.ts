import * as interfaces from './interfaces';

// Document is a relaxed signature for a Flow document of any kind.
export type Document = unknown;
// Lambda is a relaxed signature implemented by all Flow transformation lambdas.
export type Lambda = (source: Document, register?: Document, previous?: Document) => Document[];

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __interfaces_module = interfaces.__module;
// Import derivation classes from their implementation modules.
import { AcmeBankBalances } from '../../examples/acmeBank.flow';

import { ExamplesCitiBikeIdleBikes } from '../../examples/citi-bike/idle-bikes.flow';

import { ExamplesCitiBikeLastSeen } from '../../examples/citi-bike/last-seen.flow';

import { ExamplesCitiBikeRidesAndRelocations } from '../../examples/citi-bike/rides-and-relocations.flow';

import { ExamplesCitiBikeStations } from '../../examples/citi-bike/stations.flow';

import { PatternsInnerJoin } from '../../examples/derive-patterns/join-inner.flow';

import { PatternsOneSidedJoin } from '../../examples/derive-patterns/join-one-sided.flow';

import { PatternsOuterJoin } from '../../examples/derive-patterns/join-outer.flow';

import { PatternsSumsDb, PatternsSumsRegister } from '../../examples/derive-patterns/summer.flow';

import { PatternsZeroCrossing } from '../../examples/derive-patterns/zero-crossing.flow';

import {
    MarketingClicksWithViews,
    MarketingPurchaseWithOffers,
    MarketingViewsWithCampaign,
} from '../../examples/marketing/flow';

import { ExamplesNetTraceServices } from '../../examples/net-trace/services.flow';

import { ExamplesReKeyStableEvents } from '../../examples/re-key/flow';

import {
    ExamplesSegmentMemberships,
    ExamplesSegmentProfiles,
    ExamplesSegmentToggles,
} from '../../examples/segment/flow';

import { ExamplesShoppingCartUpdatesWithProducts } from '../../examples/shopping/cart-updates-with-products.flow';

import { ExamplesShoppingCarts } from '../../examples/shopping/carts.flow';

import { ExamplesShoppingPurchases } from '../../examples/shopping/purchases.flow';

import {
    SoakSetOpsSets,
    SoakSetOpsSetsRegister,
    SoakSetOpsVerify,
} from '../../examples/soak-tests/set-ops/set-ops.flow';

import { ExamplesSourceSchemaRestrictive } from '../../examples/source-schema/flow';

import { StockDailyStats } from '../../examples/stock-stats/flow';

import { TemperatureAverageByLocation, TemperatureAverageTemps } from '../../examples/temp-sensors/flow';

import { ExamplesWikiPages } from '../../examples/wiki/pages.flow';

// Build instances of each class, which will be bound to this module's router.
const __AcmeBankBalances: interfaces.AcmeBankBalances = new AcmeBankBalances();
const __ExamplesCitiBikeIdleBikes: interfaces.ExamplesCitiBikeIdleBikes = new ExamplesCitiBikeIdleBikes();
const __ExamplesCitiBikeLastSeen: interfaces.ExamplesCitiBikeLastSeen = new ExamplesCitiBikeLastSeen();
const __ExamplesCitiBikeRidesAndRelocations: interfaces.ExamplesCitiBikeRidesAndRelocations = new ExamplesCitiBikeRidesAndRelocations();
const __ExamplesCitiBikeStations: interfaces.ExamplesCitiBikeStations = new ExamplesCitiBikeStations();
const __ExamplesNetTraceServices: interfaces.ExamplesNetTraceServices = new ExamplesNetTraceServices();
const __ExamplesReKeyStableEvents: interfaces.ExamplesReKeyStableEvents = new ExamplesReKeyStableEvents();
const __ExamplesSegmentMemberships: interfaces.ExamplesSegmentMemberships = new ExamplesSegmentMemberships();
const __ExamplesSegmentProfiles: interfaces.ExamplesSegmentProfiles = new ExamplesSegmentProfiles();
const __ExamplesSegmentToggles: interfaces.ExamplesSegmentToggles = new ExamplesSegmentToggles();
const __ExamplesShoppingCartUpdatesWithProducts: interfaces.ExamplesShoppingCartUpdatesWithProducts = new ExamplesShoppingCartUpdatesWithProducts();
const __ExamplesShoppingCarts: interfaces.ExamplesShoppingCarts = new ExamplesShoppingCarts();
const __ExamplesShoppingPurchases: interfaces.ExamplesShoppingPurchases = new ExamplesShoppingPurchases();
const __ExamplesSourceSchemaRestrictive: interfaces.ExamplesSourceSchemaRestrictive = new ExamplesSourceSchemaRestrictive();
const __ExamplesWikiPages: interfaces.ExamplesWikiPages = new ExamplesWikiPages();
const __MarketingClicksWithViews: interfaces.MarketingClicksWithViews = new MarketingClicksWithViews();
const __MarketingPurchaseWithOffers: interfaces.MarketingPurchaseWithOffers = new MarketingPurchaseWithOffers();
const __MarketingViewsWithCampaign: interfaces.MarketingViewsWithCampaign = new MarketingViewsWithCampaign();
const __PatternsInnerJoin: interfaces.PatternsInnerJoin = new PatternsInnerJoin();
const __PatternsOneSidedJoin: interfaces.PatternsOneSidedJoin = new PatternsOneSidedJoin();
const __PatternsOuterJoin: interfaces.PatternsOuterJoin = new PatternsOuterJoin();
const __PatternsSumsDb: interfaces.PatternsSumsDb = new PatternsSumsDb();
const __PatternsSumsRegister: interfaces.PatternsSumsRegister = new PatternsSumsRegister();
const __PatternsZeroCrossing: interfaces.PatternsZeroCrossing = new PatternsZeroCrossing();
const __SoakSetOpsSets: interfaces.SoakSetOpsSets = new SoakSetOpsSets();
const __SoakSetOpsSetsRegister: interfaces.SoakSetOpsSetsRegister = new SoakSetOpsSetsRegister();
const __SoakSetOpsVerify: interfaces.SoakSetOpsVerify = new SoakSetOpsVerify();
const __StockDailyStats: interfaces.StockDailyStats = new StockDailyStats();
const __TemperatureAverageByLocation: interfaces.TemperatureAverageByLocation = new TemperatureAverageByLocation();
const __TemperatureAverageTemps: interfaces.TemperatureAverageTemps = new TemperatureAverageTemps();

// Now build the router that's used for transformation lambda dispatch.
const routes: { [path: string]: Lambda | undefined } = {
    '/derive/acmeBank/balances/fromTransfers/Publish': __AcmeBankBalances.fromTransfersPublish.bind(
        __AcmeBankBalances,
    ) as Lambda,
    '/derive/examples/citi-bike/idle-bikes/delayedRides/Publish': __ExamplesCitiBikeIdleBikes.delayedRidesPublish.bind(
        __ExamplesCitiBikeIdleBikes,
    ) as Lambda,
    '/derive/examples/citi-bike/idle-bikes/liveRides/Update': __ExamplesCitiBikeIdleBikes.liveRidesUpdate.bind(
        __ExamplesCitiBikeIdleBikes,
    ) as Lambda,
    '/derive/examples/citi-bike/last-seen/locationFromRide/Publish': __ExamplesCitiBikeLastSeen.locationFromRidePublish.bind(
        __ExamplesCitiBikeLastSeen,
    ) as Lambda,
    '/derive/examples/citi-bike/rides-and-relocations/fromRides/Update': __ExamplesCitiBikeRidesAndRelocations.fromRidesUpdate.bind(
        __ExamplesCitiBikeRidesAndRelocations,
    ) as Lambda,
    '/derive/examples/citi-bike/rides-and-relocations/fromRides/Publish': __ExamplesCitiBikeRidesAndRelocations.fromRidesPublish.bind(
        __ExamplesCitiBikeRidesAndRelocations,
    ) as Lambda,
    '/derive/examples/citi-bike/stations/ridesAndMoves/Publish': __ExamplesCitiBikeStations.ridesAndMovesPublish.bind(
        __ExamplesCitiBikeStations,
    ) as Lambda,
    '/derive/examples/net-trace/services/fromPairs/Publish': __ExamplesNetTraceServices.fromPairsPublish.bind(
        __ExamplesNetTraceServices,
    ) as Lambda,
    '/derive/examples/re-key/stable_events/fromAnonymousEvents/Update': __ExamplesReKeyStableEvents.fromAnonymousEventsUpdate.bind(
        __ExamplesReKeyStableEvents,
    ) as Lambda,
    '/derive/examples/re-key/stable_events/fromAnonymousEvents/Publish': __ExamplesReKeyStableEvents.fromAnonymousEventsPublish.bind(
        __ExamplesReKeyStableEvents,
    ) as Lambda,
    '/derive/examples/re-key/stable_events/fromIdMappings/Update': __ExamplesReKeyStableEvents.fromIdMappingsUpdate.bind(
        __ExamplesReKeyStableEvents,
    ) as Lambda,
    '/derive/examples/re-key/stable_events/fromIdMappings/Publish': __ExamplesReKeyStableEvents.fromIdMappingsPublish.bind(
        __ExamplesReKeyStableEvents,
    ) as Lambda,
    '/derive/examples/segment/memberships/fromSegmentation/Publish': __ExamplesSegmentMemberships.fromSegmentationPublish.bind(
        __ExamplesSegmentMemberships,
    ) as Lambda,
    '/derive/examples/segment/profiles/fromSegmentation/Publish': __ExamplesSegmentProfiles.fromSegmentationPublish.bind(
        __ExamplesSegmentProfiles,
    ) as Lambda,
    '/derive/examples/segment/toggles/fromSegmentation/Update': __ExamplesSegmentToggles.fromSegmentationUpdate.bind(
        __ExamplesSegmentToggles,
    ) as Lambda,
    '/derive/examples/segment/toggles/fromSegmentation/Publish': __ExamplesSegmentToggles.fromSegmentationPublish.bind(
        __ExamplesSegmentToggles,
    ) as Lambda,
    '/derive/examples/shopping/cartUpdatesWithProducts/cartUpdates/Publish': __ExamplesShoppingCartUpdatesWithProducts.cartUpdatesPublish.bind(
        __ExamplesShoppingCartUpdatesWithProducts,
    ) as Lambda,
    '/derive/examples/shopping/cartUpdatesWithProducts/products/Update': __ExamplesShoppingCartUpdatesWithProducts.productsUpdate.bind(
        __ExamplesShoppingCartUpdatesWithProducts,
    ) as Lambda,
    '/derive/examples/shopping/carts/cartUpdatesWithProducts/Update': __ExamplesShoppingCarts.cartUpdatesWithProductsUpdate.bind(
        __ExamplesShoppingCarts,
    ) as Lambda,
    '/derive/examples/shopping/carts/cartUpdatesWithProducts/Publish': __ExamplesShoppingCarts.cartUpdatesWithProductsPublish.bind(
        __ExamplesShoppingCarts,
    ) as Lambda,
    '/derive/examples/shopping/carts/clearAfterPurchase/Update': __ExamplesShoppingCarts.clearAfterPurchaseUpdate.bind(
        __ExamplesShoppingCarts,
    ) as Lambda,
    '/derive/examples/shopping/carts/clearAfterPurchase/Publish': __ExamplesShoppingCarts.clearAfterPurchasePublish.bind(
        __ExamplesShoppingCarts,
    ) as Lambda,
    '/derive/examples/shopping/purchases/carts/Update': __ExamplesShoppingPurchases.cartsUpdate.bind(
        __ExamplesShoppingPurchases,
    ) as Lambda,
    '/derive/examples/shopping/purchases/purchaseActions/Publish': __ExamplesShoppingPurchases.purchaseActionsPublish.bind(
        __ExamplesShoppingPurchases,
    ) as Lambda,
    '/derive/examples/source-schema/restrictive/fromPermissive/Publish': __ExamplesSourceSchemaRestrictive.fromPermissivePublish.bind(
        __ExamplesSourceSchemaRestrictive,
    ) as Lambda,
    '/derive/examples/wiki/pages/rollUpEdits/Publish': __ExamplesWikiPages.rollUpEditsPublish.bind(
        __ExamplesWikiPages,
    ) as Lambda,
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
    '/derive/patterns/inner-join/fromInts/Update': __PatternsInnerJoin.fromIntsUpdate.bind(
        __PatternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/inner-join/fromInts/Publish': __PatternsInnerJoin.fromIntsPublish.bind(
        __PatternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/inner-join/fromStrings/Update': __PatternsInnerJoin.fromStringsUpdate.bind(
        __PatternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/inner-join/fromStrings/Publish': __PatternsInnerJoin.fromStringsPublish.bind(
        __PatternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/one-sided-join/publishLHS/Publish': __PatternsOneSidedJoin.publishLHSPublish.bind(
        __PatternsOneSidedJoin,
    ) as Lambda,
    '/derive/patterns/one-sided-join/updateRHS/Update': __PatternsOneSidedJoin.updateRHSUpdate.bind(
        __PatternsOneSidedJoin,
    ) as Lambda,
    '/derive/patterns/outer-join/fromInts/Publish': __PatternsOuterJoin.fromIntsPublish.bind(
        __PatternsOuterJoin,
    ) as Lambda,
    '/derive/patterns/outer-join/fromStrings/Publish': __PatternsOuterJoin.fromStringsPublish.bind(
        __PatternsOuterJoin,
    ) as Lambda,
    '/derive/patterns/sums-db/fromInts/Publish': __PatternsSumsDb.fromIntsPublish.bind(__PatternsSumsDb) as Lambda,
    '/derive/patterns/sums-register/fromInts/Update': __PatternsSumsRegister.fromIntsUpdate.bind(
        __PatternsSumsRegister,
    ) as Lambda,
    '/derive/patterns/sums-register/fromInts/Publish': __PatternsSumsRegister.fromIntsPublish.bind(
        __PatternsSumsRegister,
    ) as Lambda,
    '/derive/patterns/zero-crossing/fromInts/Update': __PatternsZeroCrossing.fromIntsUpdate.bind(
        __PatternsZeroCrossing,
    ) as Lambda,
    '/derive/patterns/zero-crossing/fromInts/Publish': __PatternsZeroCrossing.fromIntsPublish.bind(
        __PatternsZeroCrossing,
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
    '/derive/soak/set-ops/verify/fromSets/Publish': __SoakSetOpsVerify.fromSetsPublish.bind(
        __SoakSetOpsVerify,
    ) as Lambda,
    '/derive/soak/set-ops/verify/fromSetsRegister/Publish': __SoakSetOpsVerify.fromSetsRegisterPublish.bind(
        __SoakSetOpsVerify,
    ) as Lambda,
    '/derive/stock/daily-stats/fromTicks/Publish': __StockDailyStats.fromTicksPublish.bind(__StockDailyStats) as Lambda,
    '/derive/temperature/averageByLocation/avgTempLocationSensors/Update': __TemperatureAverageByLocation.avgTempLocationSensorsUpdate.bind(
        __TemperatureAverageByLocation,
    ) as Lambda,
    '/derive/temperature/averageByLocation/avgTempLocationSensors/Publish': __TemperatureAverageByLocation.avgTempLocationSensorsPublish.bind(
        __TemperatureAverageByLocation,
    ) as Lambda,
    '/derive/temperature/averageByLocation/avgTempLocationTemps/Update': __TemperatureAverageByLocation.avgTempLocationTempsUpdate.bind(
        __TemperatureAverageByLocation,
    ) as Lambda,
    '/derive/temperature/averageByLocation/avgTempLocationTemps/Publish': __TemperatureAverageByLocation.avgTempLocationTempsPublish.bind(
        __TemperatureAverageByLocation,
    ) as Lambda,
    '/derive/temperature/averageTemps/averageTemps/Publish': __TemperatureAverageTemps.averageTempsPublish.bind(
        __TemperatureAverageTemps,
    ) as Lambda,
};

export { routes };
