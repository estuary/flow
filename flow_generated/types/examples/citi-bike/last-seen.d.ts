// Generated from collection schema examples/citi-bike/last-seen.flow.yaml?ptr=/collections/examples~1citi-bike~1last-seen/schema.
// Referenced from examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen.
export type Document = {
    bike_id: /* Unique identifier for this bike */ number;
    last: /* Station and time at which a trip began or ended */ {
        station: /* A Citi Bike Station */ {
            geo?: /* Location of this station Geographic Location as Latitude & Longitude */ {
                latitude: number;
                longitude: number;
            };
            id: /* Unique identifier for this station */ number;
            name: /* Human-friendly name of this station */ string;
        };
        timestamp: /* Timestamp as YYYY-MM-DD HH:MM:SS.F in UTC */ string;
    };
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
