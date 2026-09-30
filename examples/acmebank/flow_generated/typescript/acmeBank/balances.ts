
// Generated for published documents of derived collection acmeBank/balances.
export type Document = {
    balance: number;
    user: string;
};


// Generated for read documents of sourced collection acmeBank/transfer-outcomes.
export type SourceFromOutcomes = {
    amount: number;
    id: number;
    outcome: /* Transfer was approved, or denied for insufficient funds. */ "approve" | "deny";
    recipient: string;
    sender: string;
    sender_balance: number;
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

    abstract fromOutcomes(read: { doc: SourceFromOutcomes }): Document[];
}
