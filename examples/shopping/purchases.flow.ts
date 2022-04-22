import { IDerivation, Document, Register, CartsSource, PurchaseActionsSource } from 'flow/examples/shopping/purchases';

// Implementation for derivation examples/shopping/purchases.flow.yaml#/collections/examples~1shopping~1purchases/derivation.
export class Derivation implements IDerivation {
    cartsUpdate(source: CartsSource): Register[] {
        return [source];
    }
    purchaseActionsPublish(source: PurchaseActionsSource, register: Register, _previous: Register): Document[] {
        return [
            {
                userId: register.userId,
                timestamp: source.timestamp,
                items: register.items,
            },
        ];
    }
}
