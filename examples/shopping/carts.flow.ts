import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation.
export class ExamplesShoppingCarts implements interfaces.ExamplesShoppingCarts {
    cartUpdatesWithProductsUpdate(
        source: collections.ExamplesShoppingCartUpdatesWithProducts,
    ): registers.ExamplesShoppingCarts[] {
        return [
            {
                userId: source.action.userId,
                cartItems: { add: [source] },
            },
        ];
    }
    cartUpdatesWithProductsPublish(
        _source: collections.ExamplesShoppingCartUpdatesWithProducts,
        register: registers.ExamplesShoppingCarts,
        _previous: registers.ExamplesShoppingCarts,
    ): collections.ExamplesShoppingCarts[] {
        return [
            {
                userId: register.userId,
                items: register.cartItems.add,
            },
        ];
    }
    clearAfterPurchaseUpdate(
        source: collections.ExamplesShoppingCartPurchaseRequests,
    ): registers.ExamplesShoppingCarts[] {
        return [
            {
                userId: source.userId,
                // The intersect property is handled by the "set" reduction strategy. Intersecting
                // with an empty array clears the set.
                cartItems: { intersect: [] },
            },
        ];
    }
    clearAfterPurchasePublish(
        source: collections.ExamplesShoppingCartPurchaseRequests,
        _register: registers.ExamplesShoppingCarts,
        _previous: registers.ExamplesShoppingCarts,
    ): collections.ExamplesShoppingCarts[] {
        return [{ userId: source.userId, items: [] }];
    }
}
