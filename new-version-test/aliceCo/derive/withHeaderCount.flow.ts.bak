import { IDerivation, OutputDocument, Register, FromRequestsSource } from 'flow/aliceCo/derive/with-header-count';

// Implementation for derivation aliceCo/derive/withHeaderCount.flow.yaml#/collections/aliceCo~1derive~1with-header-count/derivation.
export class Derivation implements IDerivation {
    fromRequestsPublish(
        source: FromRequestsSource,
        _register: Register,
        _previous: Register,
    ): OutputDocument[] {
        const headerCount = source._meta.headers ? Object.keys(source._meta.headers).length : 0;
        return [{
            headerCount,
            ...source
        }]
    }
}
