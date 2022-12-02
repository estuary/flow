// Generated from $anchor schema examples/segment/derived.schema.yaml#SegmentDetail."
export type SegmentDetail = /* Status of a user's membership within a segment. */ {
    first?: /* Time at which this user was first added to this segment. */ string;
    last: /* Time at which this user was last updated within this segment. */ string;
    member: /* Is the user a current segment member? */ boolean;
    segment: Segment;
    value?: /* Most recent associated value. */ string;
};

// Generated from $anchor schema examples/segment/derived.schema.yaml#SegmentSet."
export type SegmentSet = SegmentDetail[];

// Generated from $anchor schema examples/segment/event.schema.yaml#Segment."
export type Segment = {
    name: /* Name of the segment, scoped to the vendor ID. */ string;
    vendor: /* Vendor ID of the segment. */ number;
};

// Generated from collection schema examples/segment/derived.schema.yaml#/$defs/membership.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1memberships.
export type Document = /* A user and their status within a single segment. */ {
    first?: /* Time at which this user was first added to this segment. */ string;
    last: /* Time at which this user was last updated within this segment. */ string;
    member: /* Is the user a current segment member? */ boolean;
    segment: Segment;
    user: string;
    value?: /* Most recent associated value. */ string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/segment/flow.yaml?ptr=/collections/examples~1segment~1memberships/derivation/register/schema.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation.
export type Register = unknown;

// Generated from transform fromSegmentation as a re-export of collection examples/segment/events.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation/transform/fromSegmentation."
import { SourceDocument as FromSegmentationSource } from './events';
export { SourceDocument as FromSegmentationSource } from './events';

// Generated from derivation examples/segment/flow.yaml#/collections/examples~1segment~1memberships/derivation.
// Required to be implemented by examples/segment/memberships.ts.
export interface IDerivation {
    fromSegmentationPublish(source: FromSegmentationSource, register: Register, previous: Register): OutputDocument[];
}
