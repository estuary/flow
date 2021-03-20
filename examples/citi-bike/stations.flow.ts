import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/citi-bike/stations.flow.yaml#/collections/examples~1citi-bike~1stations/derivation.
export class ExamplesCitiBikeStations implements interfaces.ExamplesCitiBikeStations {
    ridesAndMovesPublish(
        source: collections.ExamplesCitiBikeRidesAndRelocations,
        _register: registers.ExamplesCitiBikeStations,
        _previous: registers.ExamplesCitiBikeStations,
    ): collections.ExamplesCitiBikeStations[] {
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
