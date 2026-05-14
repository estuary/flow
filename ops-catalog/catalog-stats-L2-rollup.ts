import { IDerivation, Document, SourceFromBaseName } from 'flow/ops/rollups/L2/catalog-stats.ts';

// Placeholder for the ops/rollups/L2/catalog-stats derivation. Production
// overwrites this module in `update_l2_reporting` with one method per
// data-plane transform; this file's content is only exercised by catalog
// tests.
export class Derivation extends IDerivation {
    fromBaseName(read: { doc: SourceFromBaseName }): Document[] {
        return [read.doc];
    }
}
