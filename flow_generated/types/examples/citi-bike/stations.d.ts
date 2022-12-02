// Generated from collection schema examples/citi-bike/station.schema.yaml.
// Referenced from examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations.
export type Document = /* A Citi Bike Station */ {
    arrival?: /* Statistics on Bike arrivals to the station */ {
        move?: /* Bikes moved to the station */ number;
        ride?: /* Bikes ridden to the station */ number;
    };
    departure?: /* Statistics on Bike departures from the station */ {
        move?: /* Bikes moved from the station */ number;
        ride?: /* Bikes ridden from the station */ number;
    };
    geo?: /* Location of this station Geographic Location as Latitude & Longitude */ {
        latitude: number;
        longitude: number;
    };
    id: /* Unique identifier for this station */ number;
    name: /* Human-friendly name of this station */ string;
    stable?: /* Set of Bike IDs which are currently at this station */ {
        [k: string]: number[];
    };
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/citi-bike/stations.flow.yaml?ptr=/collections/examples~1citi-bike~1stations/derivation/register/schema.
// Referenced from examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations/derivation.
export type Register = unknown;

// Generated from transform ridesAndMoves as a re-export of collection examples/citi-bike/rides-and-relocations.
// Referenced from examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations/derivation/transform/ridesAndMoves."
import { SourceDocument as RidesAndMovesSource } from './rides-and-relocations';
export { SourceDocument as RidesAndMovesSource } from './rides-and-relocations';

// Generated from derivation examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations/derivation.
// Required to be implemented by examples/citi-bike/stations.flow.ts.
export interface IDerivation {
    ridesAndMovesPublish(source: RidesAndMovesSource, register: Register, previous: Register): OutputDocument[];
}
