import {
    IDerivation,
    Document,
    Register,
    SegmentDetail,
    FromSegmentationSource,
} from 'flow/examples/segment/memberships';

// Implementation for derivation examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation.
export class Derivation implements IDerivation {
    fromSegmentationPublish(source: FromSegmentationSource, _register: Register, _previous: Register): Document[] {
        const rest = {
            segment: source.segment,
            last: source.timestamp,
        };
        let detail: SegmentDetail;

        if (source.remove) {
            detail = { member: false, ...rest };
        } else {
            detail = { member: true, first: source.timestamp, value: source.value, ...rest };
        }

        return [{ user: source.user, ...detail }];
    }
}
