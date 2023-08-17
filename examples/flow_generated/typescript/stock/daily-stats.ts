
// Generated for published documents of derived collection stock/daily-stats.
export type Document = /* Daily statistics of a market security. */ {
    ask?: DocumentPriceStats;
    bid?: DocumentPriceStats;
    date: string;
    exchange: DocumentExchange;
    first?: /* First trade of the day. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    last?: /* Last trade of the day. */ {
        price: /* Dollar price. */ number;
        size: /* Number of shares. */ number;
    };
    price?: DocumentPriceStats;
    security: DocumentSecurity;
    spread?: DocumentPriceStats;
    volume?: /* Total number of shares transacted. */ number;
};


// Generated for schema $anchor PriceAndSize."
export type DocumentPriceAndSize = {
    price: /* Dollar price. */ number;
    size: /* Number of shares. */ number;
};


// Generated for schema $anchor Security."
export type DocumentSecurity = /* Market security ticker name. */ string;


// Generated for schema $anchor PriceStats."
export type DocumentPriceStats = {
    avgD?: /* Denominator of average. */ number;
    avgN?: /* Numerator of average. */ number;
    high?: number;
    low?: number;
};


// Generated for schema $anchor Exchange."
export type DocumentExchange = /* Enum of market exchange codes. */ "NASDAQ" | "NYSE" | "SEHK";


// Generated for read documents of sourced collection stock/ticks.
export type SourceFromTicks = /* Level-one market tick of a security. */ {
    "_meta"?: Record<string, unknown>;
    ask: SourceFromTicksPriceAndSize;
    bid: SourceFromTicksPriceAndSize;
    exchange: SourceFromTicksExchange;
    last: SourceFromTicksPriceAndSize;
    security: SourceFromTicksSecurity;
    time: string;
    [k: string]: Record<string, unknown> | boolean | string | null | undefined;
};


// Generated for schema $anchor PriceAndSize."
export type SourceFromTicksPriceAndSize = {
    price: /* Dollar price. */ number;
    size: /* Number of shares. */ number;
};


// Generated for schema $anchor Security."
export type SourceFromTicksSecurity = /* Market security ticker name. */ string;


// Generated for schema $anchor Exchange."
export type SourceFromTicksExchange = /* Enum of market exchange codes. */ "NASDAQ" | "NYSE" | "SEHK";


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

    abstract fromTicks(read: { doc: SourceFromTicks }): Document[];
}
