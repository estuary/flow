import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen/derivation.
export class ExamplesCitiBikeLastSeen implements interfaces.ExamplesCitiBikeLastSeen {
    locationFromRidePublish(
        source: collections.ExamplesCitiBikeRides,
        _register: registers.ExamplesCitiBikeLastSeen,
        _previous: registers.ExamplesCitiBikeLastSeen,
    ): collections.ExamplesCitiBikeLastSeen[] {
        return [{ bike_id: source.bike_id, last: source.end }];
    }
}
