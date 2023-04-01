
// Document is a relaxed signature for a Flow document of any kind.
export type Document = unknown;
// Lambda is a relaxed signature implemented by all Flow transformation lambdas.
export type Lambda = (source: Document, register?: Document, previous?: Document) => Document[];

// Import derivation classes from their implementation modules.
import { Derivation as aliceCoDeriveWithHeaderCount } from '../../aliceCo/derive/withHeaderCount.flow';

// Build instances of each class, which will be bound to this module's router.
const __aliceCoDeriveWithHeaderCount: aliceCoDeriveWithHeaderCount = new aliceCoDeriveWithHeaderCount();

// Now build the router that's used for transformation lambda dispatch.
const routes: { [path: string]: Lambda | undefined } = {
    '/derive/aliceCo/derive/with-header-count/fromRides/Publish': __aliceCoDeriveWithHeaderCount.fromRidesPublish.bind(
        __aliceCoDeriveWithHeaderCount,
    ) as Lambda,
};

export { routes };
