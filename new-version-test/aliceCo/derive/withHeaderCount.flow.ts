import { IDerivation, Document, SourceFromRequests } from 'flow/aliceCo/derive/with-header-count.ts';

// Implementation for derivation aliceCo/derive/with-header-count.
export class Derivation extends IDerivation {
    fromRequests(source: { doc: SourceFromRequests }): Document[] {
        const headerCount = source.doc._meta.headers ? Object.keys(source.doc._meta.headers).length : 0;
        return [{
            headerCount,
            ...source.doc
        }]
    }
}
