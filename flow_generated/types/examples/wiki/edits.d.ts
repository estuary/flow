// Generated from collection schema examples/wiki/edits.flow.yaml?ptr=/collections/examples~1wiki~1edits/schema.
// Referenced from examples/wiki/edits.flow.yaml#/collections/examples~1wiki~1edits.
export type Document = {
    added?: number;
    channel: string;
    countryIsoCode?: string | null;
    deleted?: number;
    page: string;
    time: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
