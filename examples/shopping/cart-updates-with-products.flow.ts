import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/shopping/cart-updates-with-products.flow.yaml#/collections/examples~1shopping~1cartUpdatesWithProducts/derivation.
export class ExamplesShoppingCartUpdatesWithProducts implements interfaces.ExamplesShoppingCartUpdatesWithProducts {
    cartUpdatesPublish(
        source: collections.ExamplesShoppingCartUpdates,
        register: registers.ExamplesShoppingCartUpdatesWithProducts,
        _previous: registers.ExamplesShoppingCartUpdatesWithProducts,
    ): collections.ExamplesShoppingCartUpdatesWithProducts[] {
        // The register schema says this might be null, so we need to deal with that here.
        // If we haven't seen a product with this id, then we simply don't publish. This makes
        // it an inner join.
        if (register) {
            return [{ action: source, product: register }];
        }
        return [];
    }
    productsUpdate(source: collections.ExamplesShoppingProducts): registers.ExamplesShoppingCartUpdatesWithProducts[] {
        return [source];
    }
}
