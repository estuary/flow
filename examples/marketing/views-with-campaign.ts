import {
    IDerivation,
    Document,
    Register,
    IndexCampaignsSource,
    JoinViewWithIndexedCampaignSource,
} from 'flow/marketing/views-with-campaign';

// Implementation for derivation examples/marketing/flow.yaml#/collections/marketing~1views-with-campaign/derivation.
export class Derivation implements IDerivation {
    indexCampaignsUpdate(source: IndexCampaignsSource): Register[] {
        return [source];
    }
    joinViewWithIndexedCampaignPublish(
        source: JoinViewWithIndexedCampaignSource,
        register: Register,
        _previous: Register,
    ): Document[] {
        return [{ ...source, campaign: register }];
    }
}
