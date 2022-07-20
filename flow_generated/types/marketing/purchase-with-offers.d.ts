// Generated from collection schema examples/marketing/flow.yaml?ptr=/collections/marketing~1purchase-with-offers/schema.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers.
export type Document = /* Purchase event joined with prior offer views and clicks. */ {
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

// Generated from derivation register schema examples/marketing/flow.yaml?ptr=/collections/marketing~1purchase-with-offers/derivation/register/schema.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation.
export type Register = {
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

// Generated from transform indexClicks as a re-export of collection marketing/clicks-with-views.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation/transform/indexClicks."
import { Document as IndexClicksSource } from './clicks-with-views';
export { Document as IndexClicksSource } from './clicks-with-views';

// Generated from transform indexViews as a re-export of collection marketing/views-with-campaign.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation/transform/indexViews."
import { Document as IndexViewsSource } from './views-with-campaign';
export { Document as IndexViewsSource } from './views-with-campaign';

// Generated from transform joinPurchaseWithViewsAndClicks as a re-export of collection marketing/purchases.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation/transform/joinPurchaseWithViewsAndClicks."
import { Document as JoinPurchaseWithViewsAndClicksSource } from './purchases';
export { Document as JoinPurchaseWithViewsAndClicksSource } from './purchases';

// Generated from derivation examples/marketing/flow.yaml#/collections/marketing~1purchase-with-offers/derivation.
// Required to be implemented by examples/marketing/purchase-with-offers.ts.
export interface IDerivation {
    indexClicksUpdate(source: IndexClicksSource): Register[];
    indexViewsUpdate(source: IndexViewsSource): Register[];
    joinPurchaseWithViewsAndClicksPublish(
        source: JoinPurchaseWithViewsAndClicksSource,
        register: Register,
        previous: Register,
    ): Document[];
}
