import { IDerivation, Document, Register, FromSegmentationSource } from 'flow/examples/segment/toggles';

// Implementation for derivation examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation.
export class Derivation implements IDerivation {
    fromSegmentationUpdate(source: FromSegmentationSource): Register[] {
        if (source.remove) {
            return [{ event: source }];
        } else {
            return [{ event: source, firstAdd: true }];
        }
    }
    fromSegmentationPublish(source: FromSegmentationSource, _register: Register, previous: Register): Document[] {
        const { event: last, firstAdd } = previous;

        // Only publish a toggle if the user has been added to the segment at
        // least once, and the |last| event add / remove status is different from
        // the source event status. This is arbitrary but reduces volume to
        // manageable levels.
        if (firstAdd && last && last.remove != source.remove) {
            return [{ previous: last, ...source }];
        }
        return [];
    }
}
