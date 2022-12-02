// Generated from $anchor schema examples/segment/event.schema.yaml#Segment."
export type Segment = {
    name: /* Name of the segment, scoped to the vendor ID. */ string;
    vendor: /* Vendor ID of the segment. */ number;
};

// Generated from collection schema examples/segment/event.schema.yaml.
// Referenced from examples/segment/flow.yaml#/collections/examples~1segment~1events.
export type Document = /* A segment event adds or removes a user into a segment. */ {
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

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
