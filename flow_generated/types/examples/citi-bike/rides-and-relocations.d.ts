// Generated from collection schema examples/citi-bike/rides-and-relocations.flow.yaml?ptr=/collections/examples~1citi-bike~1rides-and-relocations/schema.
// Referenced from examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations.
export type Document = /* Ride within the Citi Bike system */ {
    begin: /* Starting point of the trip Station and time at which a trip began or ended */ {
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
    bike_id: /* Unique identifier for this bike */ number;
    birth_year?: /* Birth year of the rider */ number | null;
    duration_seconds?: /* Duration of the trip, in seconds */ number;
    end: /* Ending point of the trip Station and time at which a trip began or ended */ {
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
    gender?: /* Gender of the rider (Zero=unknown; 1=male; 2=female) */ 0 | 1 | 2;
    relocation?: true;
    user_type?: /* Subscriber, or pay-as-you-go Customer */ null | 'Customer' | 'Subscriber';
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/citi-bike/ride.schema.yaml#/$defs/terminus.
// Referenced from examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations/derivation.
export type Register = /* Station and time at which a trip began or ended */ {
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

// Generated from transform fromRides as a re-export of collection examples/citi-bike/rides.
// Referenced from examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations/derivation/transform/fromRides."
import { SourceDocument as FromRidesSource } from './rides';
export { SourceDocument as FromRidesSource } from './rides';

// Generated from derivation examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations/derivation.
// Required to be implemented by examples/citi-bike/rides-and-relocations.flow.ts.
export interface IDerivation {
    fromRidesUpdate(source: FromRidesSource): Register[];
    fromRidesPublish(source: FromRidesSource, register: Register, previous: Register): OutputDocument[];
}
