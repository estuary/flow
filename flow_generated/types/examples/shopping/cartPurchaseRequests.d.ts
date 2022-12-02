// Generated from collection schema examples/shopping/cart-purchase-requests.flow.yaml?ptr=/collections/examples~1shopping~1cartPurchaseRequests/schema.
// Referenced from examples/shopping/cart-purchase-requests.flow.yaml#/collections/examples~1shopping~1cartPurchaseRequests.
export type Document = /* Represents a request from a user to purchase the items in their cart. */ {
    timestamp: string;
    userId: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
