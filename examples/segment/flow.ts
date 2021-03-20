import { anchors, collections, interfaces, registers } from 'flow/modules';

// detail maps a segment event into a SegmentDetail.
function detail(event: collections.ExamplesSegmentEvents): anchors.SegmentDetail {
    let rest = {
        segment: event.segment,
        last: event.timestamp,
    };
    if (event.remove) {
        return { member: false, ...rest };
    } else {
        return { member: true, first: event.timestamp, value: event.value, ...rest };
    }
}

// Implementation for derivation examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation.
export class ExamplesSegmentMemberships implements interfaces.ExamplesSegmentMemberships {
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        _register: registers.ExamplesSegmentMemberships,
        _previous: registers.ExamplesSegmentMemberships,
    ): collections.ExamplesSegmentMemberships[] {
        return [{ user: source.user, ...detail(source) }];
    }
}

// Implementation for derivation examples/segment/flow.yaml#/collections/examples~1segment~1profiles/derivation.
export class ExamplesSegmentProfiles implements interfaces.ExamplesSegmentProfiles {
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        _register: registers.ExamplesSegmentProfiles,
        _previous: registers.ExamplesSegmentProfiles,
    ): collections.ExamplesSegmentProfiles[] {
        // Each source is a segment set of O(1), which is combined with others.
        return [{ user: source.user, segments: [detail(source)] }];
    }

    /*
    // Uncomment me in tandem with trying out the "push" version of profiles in flow.yaml.

    fromSegmentationUpdate(source: collections.ExamplesSegmentEvents): anchors.SegmentSet[] {
        // Each source is a segment set of O(1), which is reduced into the register.
        return [[detail(source)]];
    }
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        register: anchors.SegmentSet,
        _previous: anchors.SegmentSet,
    ): collections.ExamplesSegmentProfiles[] {
        // Join the user with their fully reduced segments from the register.
        return [{ user: source.user, segments: register }];
    }
    */
}

// Implementation for derivation examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation.
export class ExamplesSegmentToggles implements interfaces.ExamplesSegmentToggles {
    fromSegmentationUpdate(source: collections.ExamplesSegmentEvents): registers.ExamplesSegmentToggles[] {
        if (source.remove) {
            return [{ event: source }];
        } else {
            return [{ event: source, firstAdd: true }];
        }
    }
    fromSegmentationPublish(
        source: collections.ExamplesSegmentEvents,
        _register: registers.ExamplesSegmentToggles,
        previous: registers.ExamplesSegmentToggles,
    ): collections.ExamplesSegmentToggles[] {
        let { event: last, firstAdd } = previous;

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
