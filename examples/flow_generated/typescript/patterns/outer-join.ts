
// Generated for published documents of derived collection patterns/outer-join.
export type Document = /* Document for join examples */ {
    Key: string;
    LHS?: number;
    RHS?: string[];
};


// Generated for schema $anchor Int."
export type DocumentInt = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};


// Generated for schema $anchor Join."
export type DocumentJoin = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};


// Generated for schema $anchor String."
export type DocumentString = /* A document that holds a string */ {
    Key: string;
    String: string;
};


// Generated for read documents of sourced collection patterns/ints.
export type SourceFromInts = SourceFromIntsInt;


// Generated for schema $anchor Int."
export type SourceFromIntsInt = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};


// Generated for schema $anchor Join."
export type SourceFromIntsJoin = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};


// Generated for schema $anchor String."
export type SourceFromIntsString = /* A document that holds a string */ {
    Key: string;
    String: string;
};


// Generated for read documents of sourced collection patterns/strings.
export type SourceFromStrings = SourceFromStringsString;


// Generated for schema $anchor Int."
export type SourceFromStringsInt = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};


// Generated for schema $anchor Join."
export type SourceFromStringsJoin = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};


// Generated for schema $anchor String."
export type SourceFromStringsString = /* A document that holds a string */ {
    Key: string;
    String: string;
};


export abstract class IDerivation {
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: { state: unknown }) { }

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {
        return [];
    }

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() { }

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: { runtimeCheckpoint: unknown }): { state?: { updated: unknown, mergePatch: boolean } } {
        return {};
    }

    abstract fromInts(read: { doc: SourceFromInts }): Document[];
    abstract fromStrings(read: { doc: SourceFromStrings }): Document[];
}
