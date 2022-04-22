import {
    IDerivation,
    Document,
    Register,
    CartUpdatesWithProductsSource,
    ClearAfterPurchaseSource,
} from 'flow/examples/shopping/carts';

// Implementation for derivation examples/shopping/carts.flow.yaml#/collections/examples~1shopping~1carts/derivation.
export class Derivation implements IDerivation {
    cartUpdatesWithProductsUpdate(source: CartUpdatesWithProductsSource): Register[] {
        return [
            {
                userId: source.action.userId,
                cartItems: { add: [source] },
            },
        ];
    }
    cartUpdatesWithProductsPublish(
        _source: CartUpdatesWithProductsSource,
        register: Register,
        _previous: Register,
    ): Document[] {
        return [
            {
                userId: register.userId,
                items: register.cartItems.add,
            },
        ];
    }
    clearAfterPurchaseUpdate(source: ClearAfterPurchaseSource): Register[] {
        return [
            {
                userId: source.userId,
                // The intersect property is handled by the "set" reduction strategy. Intersecting
                // with an empty array clears the set.
                cartItems: { intersect: [] },
            },
        ];
    }
    clearAfterPurchasePublish(source: ClearAfterPurchaseSource, _register: Register, _previous: Register): Document[] {
        return [{ userId: source.userId, items: [] }];
    }
}
