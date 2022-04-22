import { IDerivation, Document, Register, FromSegmentationSource, SegmentDetail } from 'flow/examples/segment/profiles';

// Implementation for derivation examples/segment/flow.yaml#/collections/examples~1segment~1profiles/derivation.
export class Derivation implements IDerivation {
    fromSegmentationPublish(source: FromSegmentationSource, _register: Register, _previous: Register): Document[] {
        // Each source is a segment set of O(1), which is combined with others.
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

        return [{ user: source.user, segments: [detail] }];
    }
}
