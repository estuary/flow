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

// Generated from derivation register schema examples/citi-bike/last-seen.flow.yaml?ptr=/collections/examples~1citi-bike~1last-seen/derivation/register/schema.
// Referenced from examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen/derivation.
export type Register = unknown;

// Generated from transform locationFromRide as a re-export of collection examples/citi-bike/rides.
// Referenced from examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen/derivation/transform/locationFromRide."
import { SourceDocument as LocationFromRideSource } from './rides';
export { SourceDocument as LocationFromRideSource } from './rides';

// Generated from derivation examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen/derivation.
// Required to be implemented by examples/citi-bike/last-seen.flow.ts.
export interface IDerivation {
    locationFromRidePublish(source: LocationFromRideSource, register: Register, previous: Register): OutputDocument[];
}
