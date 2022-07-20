// Generated from collection schema examples/shopping/cart-update.schema.yaml.
// Referenced from examples/shopping/cart-updates.flow.yaml#/collections/examples~1shopping~1cartUpdates.
export type Document = /* Represents a request from a user to add or remove a product in their cart. */ {
    productId: number;
    quantity: /* The amount to adjust, which can be negative to remove items. */ number;
    userId: number;
};
