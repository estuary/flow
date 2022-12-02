// Generated from $anchor schema examples/segment/event.schema.yaml#Segment."
export type Segment = {
    name: /* Name of the segment, scoped to the vendor ID. */ string;
    vendor: /* Vendor ID of the segment. */ number;
};

// Generated from collection schema examples/segment/flow.yaml?ptr=/collections/examples~1segment~1toggles/schema.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1toggles.
export type Document = /* A segment event adds or removes a user into a segment. */ {
    event: /* V4 UUID of the event. */ string;
    previous: /* A segment event adds or removes a user into a segment. */ {
        event: /* V4 UUID of the event. */ string;
        remove?: /* User is removed from the segment. May be unset or "true", but not "false" */ true;
        segment: {
            name: /* Name of the segment, scoped to the vendor ID. */ string;
            vendor: /* Vendor ID of the segment. */ number;
        };
        timestamp: /* RFC 3339 timestamp of the segmentation. */ string;
        user: /* User ID. */ string;
        value?: /* Associated value of the segmentation. */ string;
    };
    remove?: /* User is removed from the segment. May be unset or "true", but not "false" */ true;
    segment: {
        name: /* Name of the segment, scoped to the vendor ID. */ string;
        vendor: /* Vendor ID of the segment. */ number;
    };
    timestamp: /* RFC 3339 timestamp of the segmentation. */ string;
    user: /* User ID. */ string;
    value?: /* Associated value of the segmentation. */ string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/segment/flow.yaml?ptr=/collections/examples~1segment~1toggles/derivation/register/schema.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation.
export type Register = {
    event?: /* A segment event adds or removes a user into a segment. */ {
        event: /* V4 UUID of the event. */ string;
        remove?: /* User is removed from the segment. May be unset or "true", but not "false" */ true;
        segment: {
            name: /* Name of the segment, scoped to the vendor ID. */ string;
            vendor: /* Vendor ID of the segment. */ number;
        };
        timestamp: /* RFC 3339 timestamp of the segmentation. */ string;
        user: /* User ID. */ string;
        value?: /* Associated value of the segmentation. */ string;
    };
    firstAdd?: true;
};

// Generated from transform fromSegmentation as a re-export of collection examples/segment/events.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation/transform/fromSegmentation."
import { SourceDocument as FromSegmentationSource } from './events';
export { SourceDocument as FromSegmentationSource } from './events';

// Generated from derivation examples/segment/flow.yaml#/collections/examples~1segment~1toggles/derivation.
// Required to be implemented by examples/segment/toggles.ts.
export interface IDerivation {
    fromSegmentationUpdate(source: FromSegmentationSource): Register[];
    fromSegmentationPublish(source: FromSegmentationSource, register: Register, previous: Register): OutputDocument[];
}
