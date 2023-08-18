import { IDerivation, Document, SourceFromInts } from 'flow/patterns/sums-reductions.ts';

// Implementation for derivation examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-db/derivation.
export class Derivation extends IDerivation {
    fromInts(read: { doc: SourceFromInts }): Document[] {
        return [{ Key: read.doc.Key, Sum: read.doc.Int }];
    }
}
