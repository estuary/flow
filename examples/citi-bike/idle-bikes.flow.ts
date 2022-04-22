import {
    IDerivation,
    Document,
    Register,
    DelayedRidesSource,
    LiveRidesSource,
} from 'flow/examples/citi-bike/idle-bikes';

// Implementation for derivation examples/citi-bike/idle-bikes.flow.yaml#/collections/examples~1citi-bike~1idle-bikes/derivation.
export class Derivation implements IDerivation {
    delayedRidesPublish(source: DelayedRidesSource, register: Register, _previous: Register): Document[] {
        // Publish if the bike hasn't moved since we processed liveRidesUpdate(source) two days ago.
        if (register === source.end.timestamp) {
            return [{ bike_id: source.bike_id, station: source.end }];
        }
        return [];
    }
    liveRidesUpdate(source: LiveRidesSource): Register[] {
        return [source.end.timestamp];
    }
}
