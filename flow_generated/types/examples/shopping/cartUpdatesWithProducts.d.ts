// Generated from collection schema examples/shopping/cart-updates-with-products.flow.yaml?ptr=/collections/examples~1shopping~1cartUpdatesWithProducts/schema.
// Referenced from examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts.
export type Document = {
    action: /* Represents a request from a user to add or remove a product in their cart. */ {
        productId: number;
        quantity: /* The amount to adjust, which can be negative to remove items. */ number;
        userId: number;
    };
    product: /* A product that is available for purchase */ {
        id: number;
        name: string;
        price: number;
    };
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/shopping/cart-updates-with-products.flow.yaml?ptr=/collections/examples~1shopping~1cartUpdatesWithProducts/derivation/register/schema.
// Referenced from examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation.
export type Register = {
    id: number;
    name: string;
    price: number;
} | null;

// Generated from transform cartUpdates as a re-export of collection examples/shopping/cartUpdates.
// Referenced from examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation/transform/cartUpdates."
import { SourceDocument as CartUpdatesSource } from './cartUpdates';
export { SourceDocument as CartUpdatesSource } from './cartUpdates';

// Generated from transform products as a re-export of collection examples/shopping/products.
// Referenced from examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation/transform/products."
import { SourceDocument as ProductsSource } from './products';
export { SourceDocument as ProductsSource } from './products';

// Generated from derivation examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation.
// Required to be implemented by examples/shopping/cart-updates-with-products.flow.ts.
export interface IDerivation {
    cartUpdatesPublish(source: CartUpdatesSource, register: Register, previous: Register): OutputDocument[];
    productsUpdate(source: ProductsSource): Register[];
}
