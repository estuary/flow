import { collections, interfaces, registers } from 'flow/modules';

import * as moment from 'moment';

export class MarketingViewsWithCampaign implements interfaces.MarketingViewsWithCampaign {
    indexCampaignsUpdate(source: collections.MarketingCampaigns): [registers.MarketingViewsWithCampaign] {
        return [source];
    }
    joinViewWithIndexedCampaignPublish(
        source: collections.MarketingOfferViews,
        register: registers.MarketingViewsWithCampaign,
        _previous: registers.MarketingViewsWithCampaign,
    ): [collections.MarketingViewsWithCampaign] {
        return [{ ...source, campaign: register }];
    }
}

export class MarketingClicksWithViews implements interfaces.MarketingClicksWithViews {
    indexViewsUpdate(source: collections.MarketingViewsWithCampaign): [registers.MarketingClicksWithViews] {
        return [source];
    }
    joinClickWithIndexedViewsPublish(
        source: collections.MarketingOfferClicks,
        register: registers.MarketingClicksWithViews,
        _previous: registers.MarketingClicksWithViews,
    ): [collections.MarketingClicksWithViews] {
        return [{ ...source, view: register }];
    }
}

export class MarketingPurchaseWithOffers implements interfaces.MarketingPurchaseWithOffers {
    indexClicksUpdate(source: collections.MarketingClicksWithViews): [registers.MarketingPurchaseWithOffers] {
        let hour = moment.utc(source.timestamp).format('YYYY-MM-DD-HH');
        return [
            {
                lastSeen: source.timestamp,
                views: {},
                clicks: { [hour]: source },
            },
        ];
    }
    indexViewsUpdate(source: collections.MarketingViewsWithCampaign): [registers.MarketingPurchaseWithOffers] {
        let day = moment.utc(source.timestamp).format('YYYY-MM-DD');
        return [
            {
                lastSeen: source.timestamp,
                views: { [day]: source },
                clicks: {},
            },
        ];
    }
    joinPurchaseWithViewsAndClicksPublish(
        source: collections.MarketingPurchases,
        register: registers.MarketingPurchaseWithOffers,
        _previous: registers.MarketingPurchaseWithOffers,
    ): [collections.MarketingPurchaseWithOffers] {
        return [
            {
                ...source,
                views: Object.values(register.views),
                clicks: Object.values(register.clicks),
            },
        ];
    }
}
