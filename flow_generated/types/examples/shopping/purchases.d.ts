// Generated from collection schema examples/shopping/purchase.schema.yaml.
// Referenced from examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases.
export type Document = /* A confirmed order for items that were in the users cart */ {
    items: /* Represents a (possibly 0) quantity of a product within the cart */ {
        product?: /* A product that is available for purchase */ {
            id: number;
            name: string;
            price: number;
        };
        quantity?: number;
    }[];
    timestamp: string;
    userId: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/shopping/cart.schema.yaml.
// Referenced from examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation.
export type Register = /* Roll up of all products that users have added to a pending purchase */ {
    items: /* Represents a (possibly 0) quantity of a product within the cart */ {
        product?: /* A product that is available for purchase */ {
            id: number;
            name: string;
            price: number;
        };
        quantity?: number;
    }[];
    userId: number;
};

// Generated from transform carts as a re-export of collection examples/shopping/carts.
// Referenced from examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation/transform/carts."
import { SourceDocument as CartsSource } from './carts';
export { SourceDocument as CartsSource } from './carts';

// Generated from transform purchaseActions as a re-export of collection examples/shopping/cartPurchaseRequests.
// Referenced from examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation/transform/purchaseActions."
import { SourceDocument as PurchaseActionsSource } from './cartPurchaseRequests';
export { SourceDocument as PurchaseActionsSource } from './cartPurchaseRequests';

// Generated from derivation examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation.
// Required to be implemented by examples/shopping/purchases.flow.ts.
export interface IDerivation {
    cartsUpdate(source: CartsSource): Register[];
    purchaseActionsPublish(source: PurchaseActionsSource, register: Register, previous: Register): OutputDocument[];
}
