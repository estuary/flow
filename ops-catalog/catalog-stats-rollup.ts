import { IDerivation, OutputDocument, Register, FromOpsUsCentral1CV1Source } from 'flow/ops.us-central1-c.v1/catalog-stats-L2';

export class Derivation implements IDerivation {
    fromOpsUsCentral1CV1Publish(
        source: FromOpsUsCentral1CV1Source,
        _register: Register,
        _previous: Register,
    ): OutputDocument[] {
        return [source];
    }
}
