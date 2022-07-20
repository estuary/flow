// Generated from collection schema examples/marketing/schema.yaml#/$defs/view-with-campaign.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign.
export type Document = /* View event joined with it's campaign. */ {
    campaign: {
        campaign_id: number;
    } | null;
    campaign_id: number;
    timestamp: string;
    user_id: string;
    view_id: string;
};

// Generated from derivation register schema examples/marketing/flow.yaml?ptr=/collections/marketing~1views-with-campaign/derivation/register/schema.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation.
export type Register = {
    campaign_id: number;
} | null;

// Generated from transform indexCampaigns as a re-export of collection marketing/campaigns.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation/transform/indexCampaigns."
import { Document as IndexCampaignsSource } from './campaigns';
export { Document as IndexCampaignsSource } from './campaigns';

// Generated from transform joinViewWithIndexedCampaign as a re-export of collection marketing/offer/views.
// Referenced from examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation/transform/joinViewWithIndexedCampaign."
import { Document as JoinViewWithIndexedCampaignSource } from './offer/views';
export { Document as JoinViewWithIndexedCampaignSource } from './offer/views';

// Generated from derivation examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation.
// Required to be implemented by examples/marketing/views-with-campaign.ts.
export interface IDerivation {
    indexCampaignsUpdate(source: IndexCampaignsSource): Register[];
    joinViewWithIndexedCampaignPublish(
        source: JoinViewWithIndexedCampaignSource,
        register: Register,
        previous: Register,
    ): Document[];
}
