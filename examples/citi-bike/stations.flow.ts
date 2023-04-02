import { IDerivation, Document, SourceRidesAndMoves } from 'flow/examples/citi-bike/stations.ts';

// Implementation for derivation examples/citi-bike/stations.
export class Derivation extends IDerivation {
    ridesAndMoves(read: { doc: SourceRidesAndMoves }): Document[] {
        const source = read.doc;

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
