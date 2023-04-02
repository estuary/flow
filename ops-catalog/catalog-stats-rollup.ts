import { IDerivation, Document, SourceFromOpsUsCentral1V1 } from 'flow/ops.us-central1.v1/catalog-stats-L2.ts';

// Implementation for derivation ops.us-central1.v1/catalog-stats-L2.
export class Derivation extends IDerivation {
    fromOpsUsCentral1V1(read: { doc: SourceFromOpsUsCentral1V1 }): Document[] {
        return [read.doc]
    }
}