// Generated from collection schema examples/shopping/cart.schema.yaml.
// Referenced from examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts.
export type Document = /* Roll up of all products that users have added to a pending purchase */ {
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

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/shopping/carts.flow.yaml?ptr=/collections/examples~1shopping~1carts/derivation/register/schema.
// Referenced from examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation.
export type Register = {
    cartItems: {
        [k: string]: /* Represents a (possibly 0) quantity of a product within the cart */ {
            product?: /* A product that is available for purchase */ {
                id: number;
                name: string;
                price: number;
            };
            quantity?: number;
        }[];
    };
    userId: number;
};

// Generated from transform cartUpdatesWithProducts as a re-export of collection examples/shopping/cartUpdatesWithProducts.
// Referenced from examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation/transform/cartUpdatesWithProducts."
import { SourceDocument as CartUpdatesWithProductsSource } from './cartUpdatesWithProducts';
export { SourceDocument as CartUpdatesWithProductsSource } from './cartUpdatesWithProducts';

// Generated from transform clearAfterPurchase as a re-export of collection examples/shopping/cartPurchaseRequests.
// Referenced from examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation/transform/clearAfterPurchase."
import { SourceDocument as ClearAfterPurchaseSource } from './cartPurchaseRequests';
export { SourceDocument as ClearAfterPurchaseSource } from './cartPurchaseRequests';

// Generated from derivation examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation.
// Required to be implemented by examples/shopping/carts.flow.ts.
export interface IDerivation {
    cartUpdatesWithProductsUpdate(source: CartUpdatesWithProductsSource): Register[];
    cartUpdatesWithProductsPublish(
        source: CartUpdatesWithProductsSource,
        register: Register,
        previous: Register,
    ): OutputDocument[];
    clearAfterPurchaseUpdate(source: ClearAfterPurchaseSource): Register[];
    clearAfterPurchasePublish(
        source: ClearAfterPurchaseSource,
        register: Register,
        previous: Register,
    ): OutputDocument[];
}
