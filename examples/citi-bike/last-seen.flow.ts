import { IDerivation, Document, Register, LocationFromRideSource } from 'flow/examples/citi-bike/last-seen';

// Implementation for derivation examples/citi-bike/last-seen.flow.yaml#/collections/examples~1citi-bike~1last-seen/derivation.
export class Derivation implements IDerivation {
    locationFromRidePublish(source: LocationFromRideSource, _register: Register, _previous: Register): Document[] {
        return [{ bike_id: source.bike_id, last: source.end }];
    }
}
