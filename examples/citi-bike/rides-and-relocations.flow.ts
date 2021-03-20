import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations/derivation.
export class ExamplesCitiBikeRidesAndRelocations implements interfaces.ExamplesCitiBikeRidesAndRelocations {
    fromRidesUpdate(source: collections.ExamplesCitiBikeRides): registers.ExamplesCitiBikeRidesAndRelocations[] {
        // Index last ride for this bike.
        return [source.end];
    }
    fromRidesPublish(
        source: collections.ExamplesCitiBikeRides,
        _register: registers.ExamplesCitiBikeRidesAndRelocations,
        previous: registers.ExamplesCitiBikeRidesAndRelocations,
    ): collections.ExamplesCitiBikeRidesAndRelocations[] {
        // Compare |previous| register value from before the update lambda was applied,
        // with the source document to determine if the bike mysteriously moved.
        if (previous.station.id != 0 && previous.station.id != source.begin.station.id) {
            return [{ bike_id: source.bike_id, begin: previous, end: source.begin, relocation: true }, source];
        }
        return [source];
    }
}
