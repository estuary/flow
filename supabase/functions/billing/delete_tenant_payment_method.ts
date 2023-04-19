import { customerQuery, StripeClient } from "./shared.ts";

export interface DeleteTenantPaymentMethodsParams {
    tenant: string;
    id: string;
}

export async function deleteTenantPaymentMethod(
    req_body: DeleteTenantPaymentMethodsParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    await StripeClient.paymentMethods.detach(req_body.id);

    return [JSON.stringify({ status: "ok" }), {
        headers: { "Content-Type": "application/json" },
        status: 200,
    }];
}
