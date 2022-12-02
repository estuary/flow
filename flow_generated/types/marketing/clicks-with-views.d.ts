// Generated from collection schema examples/marketing/schema.yaml#/$defs/click-with-view.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views.
export type Document = /* Click event joined with it's view. */ {
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

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/marketing/flow.yaml?ptr=/collections/marketing~1clicks-with-views/derivation/register/schema.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation.
export type Register = {
    campaign: {
        campaign_id: number;
    } | null;
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
} | null;

// Generated from transform indexViews as a re-export of collection marketing/views-with-campaign.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation/transform/indexViews."
import { SourceDocument as IndexViewsSource } from './views-with-campaign';
export { SourceDocument as IndexViewsSource } from './views-with-campaign';

// Generated from transform joinClickWithIndexedViews as a re-export of collection marketing/offer/clicks.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation/transform/joinClickWithIndexedViews."
import { SourceDocument as JoinClickWithIndexedViewsSource } from './offer/clicks';
export { SourceDocument as JoinClickWithIndexedViewsSource } from './offer/clicks';

// Generated from derivation examples/marketing/flow.yaml#/collections/marketing~1clicks-with-views/derivation.
// Required to be implemented by examples/marketing/clicks-with-views.ts.
export interface IDerivation {
    indexViewsUpdate(source: IndexViewsSource): Register[];
    joinClickWithIndexedViewsPublish(
        source: JoinClickWithIndexedViewsSource,
        register: Register,
        previous: Register,
    ): OutputDocument[];
}
