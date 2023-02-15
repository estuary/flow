import { IDerivation, OutputDocument, Register, FromDATAPLANESource } from 'flow/ops/catalog-stats-L2';

// Implementation for derivation template-common.yaml#/collections/ops~1catalog-stats-L2/derivation.
export class Derivation implements IDerivation {
    fromDATAPLANEPublish(
        source: FromDATAPLANESource,
        _register: Register,
        _previous: Register,
    ): OutputDocument[] {
        return [source];
    }
}
