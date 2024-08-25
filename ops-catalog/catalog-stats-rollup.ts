import { IDerivation, Document, SourceFromBaseName } from 'flow/ops.us-central1.v1/catalog-stats-L2.ts';

// Implementation for derivation ops.us-central1.v1/catalog-stats-L2.
export class Derivation extends IDerivation {
    fromBaseName(read: { doc: SourceFromBaseName }): Document[] {
        return [read.doc]
    }
}