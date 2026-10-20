// Protocol types of derivations, written into every derivation project by
// Estuary.
//
// They mirror the Estuary CDK's `estuary_cdk.derive` models of Python, and are
// the protocol's JSON messages. A field which the protocol omits when empty or
// zero is optional. Messages aren't validated at runtime.
import type { CollectionSpec, ConnectorStateUpdate, RangeSpec, UUIDParts } from "./estuary.ts";

// JSON type of a component of a derivation's shuffle key.
export type ShuffleType = "BOOLEAN" | "INTEGER" | "STRING";

// A transform names its source `collection` inline, or through its message's
// `linkedCollections` table (the indirect form). Messages are linked before
// the derivation sees them: `collection` references its entry of the table,
// so it's set in either form.
export type DerivationTransform<R> = {
    name: string;
    collection: CollectionSpec;
    collectionIndex?: number;
    // The transform's `lambda`, which is `{}` if omitted.
    lambdaConfig: R;
    shuffleKey?: string[];
    readOnly?: boolean;
    backfill?: number;
};

export type Derivation<C, R> = {
    config: C;
    transforms: DerivationTransform<R>[];
    shuffleKeyTypes?: ShuffleType[];
    linkedCollections?: CollectionSpec[];
};

export type DerivedCollectionSpec<C, R> = CollectionSpec & {
    derivation: Derivation<C, R>;
};

// Request.Open, from which a derivation is constructed.
export type Open<C, R, S> = {
    collection: DerivedCollectionSpec<C, R>;
    version: string;
    range: RangeSpec;
    state: S;
};

export type ValidateTransform<R> = {
    name: string;
    collection: CollectionSpec;
    collectionIndex?: number;
    // The transform's `lambda`, which is `{}` if omitted.
    lambdaConfig: R;
    backfill?: number;
};

// Request.Validate of a derivation which is being published.
export type Validate<C, R> = {
    config: C;
    // The derived collection.
    collection: CollectionSpec;
    transforms: ValidateTransform<R>[];
    shuffleKeyTypes?: ShuffleType[];
    linkedCollections?: CollectionSpec[];
    // The last published collection, if any.
    lastCollection?: CollectionSpec;
    lastVersion?: string;
};

// Request.Read of a document by transform `transform`.
export type Read<D> = {
    transform?: number;
    uuid: UUIDParts;
    shuffle?: {
        // Shuffle key of the document, as a JSON array.
        key?: unknown[];
        hash?: number;
    };
    doc: D;
};

// Request.Flush of the current transaction.
export type Flush = {
    // Aggregated connector state patches contributed by all participating
    // shards in the previous Flush iteration of the current transaction.
    // It's empty on the first iteration.
    statePatches?: unknown[];
};

// Response.Flushed of the current transaction.
export type Flushed<S> = {
    // Connector state update to contribute this transaction. It's aggregated
    // across shards and fed back through `Flush.statePatches`.
    state?: ConnectorStateUpdate<S>;
    // Request a further Flush iteration this transaction.
    more?: boolean;
};

// Response.Validated, having each transform in the order of Validate.
// A `readOnly` transform never publishes documents.
export type Validated = {
    transforms: { readOnly?: boolean }[];
};

// Link each of `transforms` to its collection within `linkedCollections`, by
// reference, or else require that it carries its own. Transforms of one
// collection then share one parsed CollectionSpec. Each omitted `lambdaConfig`
// becomes `{}`.
export function linkTransforms(
    transforms: { collection?: CollectionSpec; collectionIndex?: number; lambdaConfig?: unknown }[],
    linkedCollections: CollectionSpec[] | undefined,
): void {
    transforms.forEach((transform, index) => {
        transform.lambdaConfig ??= {};

        if (!linkedCollections || linkedCollections.length == 0) {
            if (transform.collection === undefined) {
                throw new Error(`transform ${index} is missing its collection`);
            }
            return;
        }
        const collectionIndex = transform.collectionIndex ?? 0;
        if (collectionIndex >= linkedCollections.length) {
            throw new Error(
                `transform ${index} has collectionIndex ${collectionIndex}, but there are ${linkedCollections.length} linked collections`,
            );
        }
        transform.collection = linkedCollections[collectionIndex];
    });
}
