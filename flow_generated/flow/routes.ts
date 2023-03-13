// Document is a relaxed signature for a Flow document of any kind.
export type Document = unknown;
// Lambda is a relaxed signature implemented by all Flow transformation lambdas.
export type Lambda = (source: Document, register?: Document, previous?: Document) => Document[];

// Import derivation classes from their implementation modules.
import { Derivation as examplesCitiBikeRides } from '../../examples/citi-bike/transform-csv-rides';
import { Derivation as examplesCitiBikeRidesAndRelocations } from '../../examples/citi-bike/rides-and-relocations.flow';
import { Derivation as examplesCitiBikeStations } from '../../examples/citi-bike/stations.flow';
import { Derivation as examplesNetTraceServices } from '../../examples/net-trace/services.flow';
import { Derivation as examplesReKeyStableEvents } from '../../examples/re-key/flow';
import { Derivation as examplesSegmentMemberships } from '../../examples/segment/memberships';
import { Derivation as examplesSegmentProfiles } from '../../examples/segment/profiles';
import { Derivation as examplesSegmentToggles } from '../../examples/segment/toggles';
import { Derivation as examplesShoppingCartUpdatesWithProducts } from '../../examples/shopping/cart-updates-with-products.flow';
import { Derivation as examplesShoppingCarts } from '../../examples/shopping/carts.flow';
import { Derivation as examplesShoppingPurchases } from '../../examples/shopping/purchases.flow';
import { Derivation as examplesSourceSchemaRestrictive } from '../../examples/source-schema/flow';
import { Derivation as examplesWikiPages } from '../../examples/wiki/pages.flow';
import { Derivation as marketingClicksWithViews } from '../../examples/marketing/clicks-with-views';
import { Derivation as marketingPurchaseWithOffers } from '../../examples/marketing/purchase-with-offers';
import { Derivation as marketingViewsWithCampaign } from '../../examples/marketing/views-with-campaign';
import { Derivation as patternsInnerJoin } from '../../examples/derive-patterns/join-inner.flow';
import { Derivation as patternsOneSidedJoin } from '../../examples/derive-patterns/join-one-sided.flow';
import { Derivation as patternsOuterJoin } from '../../examples/derive-patterns/join-outer.flow';
import { Derivation as patternsSumsDb } from '../../examples/derive-patterns/summer.flow';
import { Derivation as patternsSumsRegister } from '../../examples/derive-patterns/summer-reg.flow';
import { Derivation as patternsZeroCrossing } from '../../examples/derive-patterns/zero-crossing.flow';
import { Derivation as soakSetOpsSets } from '../../examples/soak-tests/set-ops/sets';
import { Derivation as soakSetOpsSetsRegister } from '../../examples/soak-tests/set-ops/sets-register';
import { Derivation as stockDailyStats } from '../../examples/stock-stats/flow';
import { Derivation as temperatureAverages } from '../../examples/temp-sensors/flow';

// Build instances of each class, which will be bound to this module's router.
const __examplesCitiBikeRides: examplesCitiBikeRides = new examplesCitiBikeRides();
const __examplesCitiBikeRidesAndRelocations: examplesCitiBikeRidesAndRelocations =
    new examplesCitiBikeRidesAndRelocations();
const __examplesCitiBikeStations: examplesCitiBikeStations = new examplesCitiBikeStations();
const __examplesNetTraceServices: examplesNetTraceServices = new examplesNetTraceServices();
const __examplesReKeyStableEvents: examplesReKeyStableEvents = new examplesReKeyStableEvents();
const __examplesSegmentMemberships: examplesSegmentMemberships = new examplesSegmentMemberships();
const __examplesSegmentProfiles: examplesSegmentProfiles = new examplesSegmentProfiles();
const __examplesSegmentToggles: examplesSegmentToggles = new examplesSegmentToggles();
const __examplesShoppingCartUpdatesWithProducts: examplesShoppingCartUpdatesWithProducts =
    new examplesShoppingCartUpdatesWithProducts();
const __examplesShoppingCarts: examplesShoppingCarts = new examplesShoppingCarts();
const __examplesShoppingPurchases: examplesShoppingPurchases = new examplesShoppingPurchases();
const __examplesSourceSchemaRestrictive: examplesSourceSchemaRestrictive = new examplesSourceSchemaRestrictive();
const __examplesWikiPages: examplesWikiPages = new examplesWikiPages();
const __marketingClicksWithViews: marketingClicksWithViews = new marketingClicksWithViews();
const __marketingPurchaseWithOffers: marketingPurchaseWithOffers = new marketingPurchaseWithOffers();
const __marketingViewsWithCampaign: marketingViewsWithCampaign = new marketingViewsWithCampaign();
const __patternsInnerJoin: patternsInnerJoin = new patternsInnerJoin();
const __patternsOneSidedJoin: patternsOneSidedJoin = new patternsOneSidedJoin();
const __patternsOuterJoin: patternsOuterJoin = new patternsOuterJoin();
const __patternsSumsDb: patternsSumsDb = new patternsSumsDb();
const __patternsSumsRegister: patternsSumsRegister = new patternsSumsRegister();
const __patternsZeroCrossing: patternsZeroCrossing = new patternsZeroCrossing();
const __soakSetOpsSets: soakSetOpsSets = new soakSetOpsSets();
const __soakSetOpsSetsRegister: soakSetOpsSetsRegister = new soakSetOpsSetsRegister();
const __stockDailyStats: stockDailyStats = new stockDailyStats();
const __temperatureAverages: temperatureAverages = new temperatureAverages();

// Now build the router that's used for transformation lambda dispatch.
const routes: { [path: string]: Lambda | undefined } = {
    '/derive/examples/citi-bike/rides/fromCsvRides/Publish': __examplesCitiBikeRides.fromCsvRidesPublish.bind(
        __examplesCitiBikeRides,
    ) as Lambda,
    '/derive/examples/citi-bike/rides-and-relocations/fromRides/Publish':
        __examplesCitiBikeRidesAndRelocations.fromRidesPublish.bind(__examplesCitiBikeRidesAndRelocations) as Lambda,
    '/derive/examples/citi-bike/stations/ridesAndMoves/Publish': __examplesCitiBikeStations.ridesAndMovesPublish.bind(
        __examplesCitiBikeStations,
    ) as Lambda,
    '/derive/examples/net-trace/services/fromPairs/Publish': __examplesNetTraceServices.fromPairsPublish.bind(
        __examplesNetTraceServices,
    ) as Lambda,
    '/derive/examples/re-key/stable_events/fromAnonymousEvents/Update':
        __examplesReKeyStableEvents.fromAnonymousEventsUpdate.bind(__examplesReKeyStableEvents) as Lambda,
    '/derive/examples/re-key/stable_events/fromAnonymousEvents/Publish':
        __examplesReKeyStableEvents.fromAnonymousEventsPublish.bind(__examplesReKeyStableEvents) as Lambda,
    '/derive/examples/re-key/stable_events/fromIdMappings/Update':
        __examplesReKeyStableEvents.fromIdMappingsUpdate.bind(__examplesReKeyStableEvents) as Lambda,
    '/derive/examples/re-key/stable_events/fromIdMappings/Publish':
        __examplesReKeyStableEvents.fromIdMappingsPublish.bind(__examplesReKeyStableEvents) as Lambda,
    '/derive/examples/segment/memberships/fromSegmentation/Publish':
        __examplesSegmentMemberships.fromSegmentationPublish.bind(__examplesSegmentMemberships) as Lambda,
    '/derive/examples/segment/profiles/fromSegmentation/Publish':
        __examplesSegmentProfiles.fromSegmentationPublish.bind(__examplesSegmentProfiles) as Lambda,
    '/derive/examples/segment/toggles/fromSegmentation/Update': __examplesSegmentToggles.fromSegmentationUpdate.bind(
        __examplesSegmentToggles,
    ) as Lambda,
    '/derive/examples/segment/toggles/fromSegmentation/Publish': __examplesSegmentToggles.fromSegmentationPublish.bind(
        __examplesSegmentToggles,
    ) as Lambda,
    '/derive/examples/shopping/cartUpdatesWithProducts/cartUpdates/Publish':
        __examplesShoppingCartUpdatesWithProducts.cartUpdatesPublish.bind(
            __examplesShoppingCartUpdatesWithProducts,
        ) as Lambda,
    '/derive/examples/shopping/cartUpdatesWithProducts/products/Update':
        __examplesShoppingCartUpdatesWithProducts.productsUpdate.bind(
            __examplesShoppingCartUpdatesWithProducts,
        ) as Lambda,
    '/derive/examples/shopping/carts/cartUpdatesWithProducts/Update':
        __examplesShoppingCarts.cartUpdatesWithProductsUpdate.bind(__examplesShoppingCarts) as Lambda,
    '/derive/examples/shopping/carts/cartUpdatesWithProducts/Publish':
        __examplesShoppingCarts.cartUpdatesWithProductsPublish.bind(__examplesShoppingCarts) as Lambda,
    '/derive/examples/shopping/carts/clearAfterPurchase/Update': __examplesShoppingCarts.clearAfterPurchaseUpdate.bind(
        __examplesShoppingCarts,
    ) as Lambda,
    '/derive/examples/shopping/carts/clearAfterPurchase/Publish':
        __examplesShoppingCarts.clearAfterPurchasePublish.bind(__examplesShoppingCarts) as Lambda,
    '/derive/examples/shopping/purchases/carts/Update': __examplesShoppingPurchases.cartsUpdate.bind(
        __examplesShoppingPurchases,
    ) as Lambda,
    '/derive/examples/shopping/purchases/purchaseActions/Publish':
        __examplesShoppingPurchases.purchaseActionsPublish.bind(__examplesShoppingPurchases) as Lambda,
    '/derive/examples/source-schema/restrictive/fromPermissive/Publish':
        __examplesSourceSchemaRestrictive.fromPermissivePublish.bind(__examplesSourceSchemaRestrictive) as Lambda,
    '/derive/examples/wiki/pages/rollUpEdits/Publish': __examplesWikiPages.rollUpEditsPublish.bind(
        __examplesWikiPages,
    ) as Lambda,
    '/derive/marketing/clicks-with-views/indexViews/Update': __marketingClicksWithViews.indexViewsUpdate.bind(
        __marketingClicksWithViews,
    ) as Lambda,
    '/derive/marketing/clicks-with-views/joinClickWithIndexedViews/Publish':
        __marketingClicksWithViews.joinClickWithIndexedViewsPublish.bind(__marketingClicksWithViews) as Lambda,
    '/derive/marketing/purchase-with-offers/indexClicks/Update': __marketingPurchaseWithOffers.indexClicksUpdate.bind(
        __marketingPurchaseWithOffers,
    ) as Lambda,
    '/derive/marketing/purchase-with-offers/indexViews/Update': __marketingPurchaseWithOffers.indexViewsUpdate.bind(
        __marketingPurchaseWithOffers,
    ) as Lambda,
    '/derive/marketing/purchase-with-offers/joinPurchaseWithViewsAndClicks/Publish':
        __marketingPurchaseWithOffers.joinPurchaseWithViewsAndClicksPublish.bind(
            __marketingPurchaseWithOffers,
        ) as Lambda,
    '/derive/marketing/views-with-campaign/indexCampaigns/Update':
        __marketingViewsWithCampaign.indexCampaignsUpdate.bind(__marketingViewsWithCampaign) as Lambda,
    '/derive/marketing/views-with-campaign/joinViewWithIndexedCampaign/Publish':
        __marketingViewsWithCampaign.joinViewWithIndexedCampaignPublish.bind(__marketingViewsWithCampaign) as Lambda,
    '/derive/patterns/inner-join/fromInts/Update': __patternsInnerJoin.fromIntsUpdate.bind(
        __patternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/inner-join/fromInts/Publish': __patternsInnerJoin.fromIntsPublish.bind(
        __patternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/inner-join/fromStrings/Update': __patternsInnerJoin.fromStringsUpdate.bind(
        __patternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/inner-join/fromStrings/Publish': __patternsInnerJoin.fromStringsPublish.bind(
        __patternsInnerJoin,
    ) as Lambda,
    '/derive/patterns/one-sided-join/publishLHS/Publish': __patternsOneSidedJoin.publishLHSPublish.bind(
        __patternsOneSidedJoin,
    ) as Lambda,
    '/derive/patterns/one-sided-join/updateRHS/Update': __patternsOneSidedJoin.updateRHSUpdate.bind(
        __patternsOneSidedJoin,
    ) as Lambda,
    '/derive/patterns/outer-join/fromInts/Publish': __patternsOuterJoin.fromIntsPublish.bind(
        __patternsOuterJoin,
    ) as Lambda,
    '/derive/patterns/outer-join/fromStrings/Publish': __patternsOuterJoin.fromStringsPublish.bind(
        __patternsOuterJoin,
    ) as Lambda,
    '/derive/patterns/sums-db/fromInts/Publish': __patternsSumsDb.fromIntsPublish.bind(__patternsSumsDb) as Lambda,
    '/derive/patterns/sums-register/fromInts/Update': __patternsSumsRegister.fromIntsUpdate.bind(
        __patternsSumsRegister,
    ) as Lambda,
    '/derive/patterns/sums-register/fromInts/Publish': __patternsSumsRegister.fromIntsPublish.bind(
        __patternsSumsRegister,
    ) as Lambda,
    '/derive/patterns/zero-crossing/fromInts/Update': __patternsZeroCrossing.fromIntsUpdate.bind(
        __patternsZeroCrossing,
    ) as Lambda,
    '/derive/patterns/zero-crossing/fromInts/Publish': __patternsZeroCrossing.fromIntsPublish.bind(
        __patternsZeroCrossing,
    ) as Lambda,
    '/derive/soak/set-ops/sets/onOperation/Publish': __soakSetOpsSets.onOperationPublish.bind(
        __soakSetOpsSets,
    ) as Lambda,
    '/derive/soak/set-ops/sets-register/onOperation/Update': __soakSetOpsSetsRegister.onOperationUpdate.bind(
        __soakSetOpsSetsRegister,
    ) as Lambda,
    '/derive/soak/set-ops/sets-register/onOperation/Publish': __soakSetOpsSetsRegister.onOperationPublish.bind(
        __soakSetOpsSetsRegister,
    ) as Lambda,
    '/derive/stock/daily-stats/fromTicks/Publish': __stockDailyStats.fromTicksPublish.bind(__stockDailyStats) as Lambda,
    '/derive/temperature/averages/fromReadings/Publish': __temperatureAverages.fromReadingsPublish.bind(
        __temperatureAverages,
    ) as Lambda,
    '/derive/temperature/averages/fromSensors/Publish': __temperatureAverages.fromSensorsPublish.bind(
        __temperatureAverages,
    ) as Lambda,
};

export { routes };
