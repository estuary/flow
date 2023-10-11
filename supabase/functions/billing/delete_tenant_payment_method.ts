import { billingResponseHeaders, customerQuery, StripeClient } from "./shared.ts";

export interface DeleteTenantPaymentMethodsParams {
    tenant: string;
    id: string;
}

export async function deleteTenantPaymentMethod(
    req_body: DeleteTenantPaymentMethodsParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    await StripeClient.paymentMethods.detach(req_body.id);

    const customer = (await StripeClient.customers.search({ query: customerQuery(req_body.tenant) })).data[0];
    if (customer) {
        const methods = (await StripeClient.customers.listPaymentMethods(customer.id)).data;
        const validMethod = methods.filter((m: { id: string }) => m.id !== req_body.id)[0];
        if (validMethod) {
            await StripeClient.customers.update(customer.id, { invoice_settings: { default_payment_method: validMethod.id } });
        }
    }
    return [JSON.stringify({ status: "ok" }), {
        headers: billingResponseHeaders,
        status: 200,
    }];
}
