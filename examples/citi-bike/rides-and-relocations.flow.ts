import { IDerivation, Document, Register, FromRidesSource } from 'flow/examples/citi-bike/rides-and-relocations';

// Implementation for derivation examples/citi-bike/rides-and-relocations.flow.yaml#/collections/examples~1citi-bike~1rides-and-relocations/derivation.
export class Derivation implements IDerivation {
    fromRidesPublish(source: FromRidesSource, _register: Register, previous: Register): Document[] {
        // Compare |previous| register value from before the update lambda was applied,
        // with the source document to determine if the bike mysteriously moved.
        if (previous.station.id != 0 && previous.station.id != source.begin.station.id) {
            return [{ bike_id: source.bike_id, begin: previous, end: source.begin, relocation: true }, source];
        }
        return [source];
    }
}
