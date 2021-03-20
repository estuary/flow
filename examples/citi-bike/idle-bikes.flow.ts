import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation.
export class ExamplesCitiBikeIdleBikes implements interfaces.ExamplesCitiBikeIdleBikes {
    delayedRidesPublish(
        source: collections.ExamplesCitiBikeRides,
        register: registers.ExamplesCitiBikeIdleBikes,
        _previous: registers.ExamplesCitiBikeIdleBikes,
    ): collections.ExamplesCitiBikeIdleBikes[] {
        // Publish if the bike hasn't moved since we processed liveRidesUpdate(source) two days ago.
        if (register === source.end.timestamp) {
            return [{ bike_id: source.bike_id, station: source.end }];
        }
        return [];
    }
    liveRidesUpdate(source: collections.ExamplesCitiBikeRides): registers.ExamplesCitiBikeIdleBikes[] {
        return [source.end.timestamp];
    }
}
