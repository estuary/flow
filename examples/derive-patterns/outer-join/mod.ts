import { IDerivation, Document, SourceFromInts, SourceFromStrings } from 'flow/patterns/outer-join.ts';

// Implementation of derivation patterns/outer-join.
export class Derivation extends IDerivation {
    fromInts(read: {doc: SourceFromInts}): Document[] {
        return [{ Key: read.doc.Key, LHS: read.doc.Int }];
    }
    fromStrings(read: {doc: SourceFromStrings}): Document[] {
        return [{ Key: read.doc.Key, RHS: [read.doc.String] }];
    }
}
