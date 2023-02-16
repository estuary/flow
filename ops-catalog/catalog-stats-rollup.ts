import { IDerivation, OutputDocument, Register, FromOpsUsCentral1V1Source } from 'flow/ops.us-central1.v1/catalog-stats-L2';

export class Derivation implements IDerivation {
    fromOpsUsCentral1V1Publish(
        source: FromOpsUsCentral1V1Source,
        _register: Register,
        _previous: Register,
    ): OutputDocument[] {
        return [source];
    }
}
