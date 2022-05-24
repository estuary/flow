import {
    IDerivation,
    Document,
    Register,
    CartUpdatesSource,
    ProductsSource,
} from 'flow/examples/shopping/cartUpdatesWithProducts';

// Implementation for derivation examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation.
export class Derivation implements IDerivation {
    cartUpdatesPublish(source: CartUpdatesSource, register: Register, _previous: Register): Document[] {
        // The register schema says this might be null, so we need to deal with that here.
        // If we haven't seen a product with this id, then we simply don't publish. This makes
        // it an inner join.
        if (register) {
            return [{ action: source, product: register }];
        }
        return [];
    }
    productsUpdate(source: ProductsSource): Register[] {
        return [source];
    }
}
