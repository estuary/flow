import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation.
export class ExamplesShoppingPurchases implements interfaces.ExamplesShoppingPurchases {
    cartsUpdate(source: collections.ExamplesShoppingCarts): registers.ExamplesShoppingPurchases[] {
        return [source];
    }
    purchaseActionsPublish(
        source: collections.ExamplesShoppingCartPurchaseRequests,
        register: registers.ExamplesShoppingPurchases,
        _previous: registers.ExamplesShoppingPurchases,
    ): collections.ExamplesShoppingPurchases[] {
        return [
            {
                userId: register.userId,
                timestamp: source.timestamp,
                items: register.items,
            },
        ];
    }
}
