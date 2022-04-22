import { IDerivation, Document, Register, RidesAndMovesSource } from 'flow/examples/citi-bike/stations';

// Implementation for derivation examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations/derivation.
export class Derivation implements IDerivation {
    ridesAndMovesPublish(source: RidesAndMovesSource, _register: Register, _previous: Register): Document[] {
        if (source.relocation) {
            return [
                {
                    departure: { move: 1 },
                    stable: { remove: [source.bike_id] },
                    ...source.begin.station,
                },
                {
                    arrival: { move: 1 },
                    stable: { add: [source.bike_id] },
                    ...source.end.station,
                },
            ];
        } else {
            return [
                {
                    departure: { ride: 1 },
                    stable: { remove: [source.bike_id] },
                    ...source.begin.station,
                },
                {
                    arrival: { ride: 1 },
                    stable: { add: [source.bike_id] },
                    ...source.end.station,
                },
            ];
        }
    }
}
