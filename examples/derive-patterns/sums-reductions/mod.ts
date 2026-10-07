import { IDerivation, Document, SourceFromInts } from 'flow/patterns/sums-reductions.ts';

// Implementation of derivation patterns/sums-reductions.
export class Derivation extends IDerivation {
    fromInts(read: { doc: SourceFromInts }): Document[] {
        return [{ Key: read.doc.Key, Sum: read.doc.Int }];
    }
}
