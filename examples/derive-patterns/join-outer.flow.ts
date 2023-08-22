import { IDerivation, Document, SourceFromInts, SourceFromStrings } from 'flow/patterns/outer-join.ts';

// Implementation for derivation examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation.
export class Derivation extends IDerivation {
    fromInts(read: {doc: SourceFromInts}): Document[] {
        return [{ Key: read.doc.Key, LHS: read.doc.Int }];
    }
    fromStrings(read: {doc: SourceFromStrings}): Document[] {
        return [{ Key: read.doc.Key, RHS: [read.doc.String] }];
    }
}
