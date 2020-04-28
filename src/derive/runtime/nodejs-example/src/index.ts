import * as estuary from "estuary_runtime";
import * as moment from "moment";

// Generated from "marketing/campaigns" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/campaign"
type MarketingCampaigns = {
  campaign_id: number;
};

// Generated from "marketing/offer/views" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/view"
type MarketingOfferViews = {
  campaign_id: number;
  timestamp: string;
  user_id: string;
  view_id: string;
};

// Generated from "marketing/offer/clicks" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/click"
type MarketingOfferClicks = {
  click_id: string;
  timestamp: string;
  user_id: string;
  view_id: string;
};

// Generated from "marketing/purchases" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/purchase"
type MarketingPurchases = {
  purchase_id: number;
  user_id: string;
};

// Generated from "marketing/views-with-campaign" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/view-with-campaign"
type MarketingViewsWithCampaign = {
  campaign_id: number;
  timestamp: string;
  user_id: string;
  view_id: string;
} & {
  campaign: {
    campaign_id: number;
  };
};

// Generated from "marketing/clicks-with-views" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/click-with-view"
type MarketingClicksWithViews = {
  click_id: string;
  timestamp: string;
  user_id: string;
  view_id: string;
} & {
  view: {
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
  } & {
    campaign: {
      campaign_id: number;
    };
  };
};

// Generated from "marketing/purchases-with-offers" @ "file:///home/johnny/estuary/examples/marketing/schemas.yaml#/$defs/purchase-with-offers"
type MarketingPurchasesWithOffers = {
  purchase_id: number;
  user_id: string;
} & {
  clicks: ({
    click_id: string;
    timestamp: string;
    user_id: string;
    view_id: string;
  } & {
    view: {
      campaign_id: number;
      timestamp: string;
      user_id: string;
      view_id: string;
    } & {
      campaign: {
        campaign_id: number;
      };
    };
  })[];
  views: ({
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
  } & {
    campaign: {
      campaign_id: number;
    };
  })[];
};

// Generated from "stock/ticks" @ "file:///home/johnny/estuary/examples/stock-stats/schemas/L1-tick.yaml"
type StockTicks = {
  ask: {
    price: number;
    size: number;
  };
  bid: {
    price: number;
    size: number;
  };
  exchange: "NYSE" | "NASDAQ" | "SEHK";
  id: string;
  last: {
    price: number;
    size: number;
  };
  security: string;
  time: string;
};

// Generated from "stock/daily-stats" @ "file:///home/johnny/estuary/examples/stock-stats/schemas/daily-stat.yaml"
type StockDailyStats = {
  ask?: {
    avgD?: number;
    avgN?: number;
    high?: number;
    low?: number;
  };
  bid?: {
    avgD?: number;
    avgN?: number;
    high?: number;
    low?: number;
  };
  date: string;
  exchange: "NYSE" | "NASDAQ" | "SEHK";
  first?: {
    price: number;
    size: number;
  };
  last?: {
    price: number;
    size: number;
  };
  price?: {
    avgD?: number;
    avgN?: number;
    high?: number;
    low?: number;
  };
  security: string;
  spread?: {
    avgD?: number;
    avgN?: number;
    high?: number;
    low?: number;
  };
  volume?: number;
};

let bootstraps: estuary.BootstrapMap = {
  9: [
    async (state: estuary.StateStore): Promise<void> => {
      console.log("example of a bootstrap!");
    },
  ],
};

let transforms: estuary.TransformMap = {
  1: async (
    doc: MarketingCampaigns,
    state: estuary.StateStore
  ): Promise<MarketingViewsWithCampaign[] | void> => {
    await state.set(`{doc.campaign_id}`, doc);
  },
  2: async (
    doc: MarketingOfferViews,
    state: estuary.StateStore
  ): Promise<MarketingViewsWithCampaign[] | void> => {
    return [{ ...doc, campaign: await state.get(`{doc.campaign_id}`) }];
  },
  3: async (
    doc: MarketingViewsWithCampaign,
    state: estuary.StateStore
  ): Promise<MarketingClicksWithViews[] | void> => {
    let ts = moment.utc(doc.timestamp);
    let expiry = ts.add({ days: 2 });
    await state.set(doc.view_id, doc, expiry.toDate());
  },
  4: async (
    doc: MarketingOfferClicks,
    state: estuary.StateStore
  ): Promise<MarketingClicksWithViews[] | void> => {
    return [{ ...doc, view: await state.get(doc.view_id) }];
  },
  5: async (
    doc: MarketingViewsWithCampaign,
    state: estuary.StateStore
  ): Promise<MarketingPurchasesWithOffers[] | void> => {
    let ts = moment.utc(doc.timestamp);
    let key = `${doc.user_id}/views/${ts.format("YYYY-MM-DD")}`;
    let expiry = ts.add({ days: 30 });
    await state.set(key, doc, expiry.toDate());
  },
  6: async (
    doc: MarketingClicksWithViews,
    state: estuary.StateStore
  ): Promise<MarketingPurchasesWithOffers[] | void> => {
    let ts = moment.utc(doc.timestamp);
    let key = `${doc.user_id}/clicks/${ts.format("yyyy-mm-dd-hh")}`;
    let expiry = ts.add({ days: 30 });
    await state.set(key, doc, expiry.toDate());
  },
  7: async (
    doc: MarketingPurchases,
    state: estuary.StateStore
  ): Promise<MarketingPurchasesWithOffers[] | void> => {
    return [
      {
        ...doc,
        views: await state.get(`{doc.user_id}/views/`),
        clicks: await state.get(`{rec.user_id}/clicks/`),
      },
    ];
  },
  8: async (
    doc: StockTicks,
    state: estuary.StateStore
  ): Promise<StockDailyStats[] | void> => {
    let spread = doc.ask.price - doc.bid.price;

    return [
      {
        exchange: doc.exchange,
        security: doc.security,
        date: moment.utc(doc.time).format("YYYY-MM-DD"),
        // Price stat uses a by-volume weighted average of trades.
        price: {
          low: doc.last.price,
          high: doc.last.price,
          avgN: doc.last.price * doc.last.size,
          avgD: doc.last.size,
        },
        // Bid, ask, and spread stats use equal weighting of observed prices across ticks.
        bid: {
          low: doc.bid.price,
          high: doc.bid.price,
          avgN: doc.bid.price,
          avgD: 1,
        },
        ask: {
          low: doc.ask.price,
          high: doc.ask.price,
          avgN: doc.ask.price,
          avgD: 1,
        },
        spread: { low: spread, high: spread, avgN: spread, avgD: 1 },
        volume: doc.last.size,
        first: doc.last,
        last: doc.last,
      },
    ];
  },
};

estuary.main(bootstraps, transforms);
