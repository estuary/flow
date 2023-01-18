// Generated from collection schema examples/citi-bike/idle-bikes.flow.yaml?ptr=/collections/examples~1citi-bike~1idle-bikes/schema.
// Referenced from examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes.
export type Document = {
    bike_id: number;
    station: /* Station and time at which a trip began or ended */ {
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

// Generated from derivation register schema examples/citi-bike/idle-bikes.flow.yaml?ptr=/collections/examples~1citi-bike~1idle-bikes/derivation/register/schema.
// Referenced from examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation.
export type Register = string | null;

// Generated from transform delayedRides as a re-export of collection examples/citi-bike/rides.
// Referenced from examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation/transform/delayedRides."
import { SourceDocument as DelayedRidesSource } from './rides';
export { SourceDocument as DelayedRidesSource } from './rides';

// Generated from transform liveRides as a re-export of collection examples/citi-bike/rides.
// Referenced from examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation/transform/liveRides."
import { SourceDocument as LiveRidesSource } from './rides';
export { SourceDocument as LiveRidesSource } from './rides';

// Generated from derivation examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation.
// Required to be implemented by examples/citi-bike/idle-bikes.flow.ts.
export interface IDerivation {
    delayedRidesPublish(source: DelayedRidesSource, register: Register, previous: Register): OutputDocument[];
    liveRidesUpdate(source: LiveRidesSource): Register[];
}
