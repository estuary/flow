import { billingResponseHeaders, customerQuery, StripeClient } from "./shared.ts";

export interface DeleteTenantPaymentMethodsParams {
    tenant: string;
    id: string;
}

export async function deleteTenantPaymentMethod(
    req_body: DeleteTenantPaymentMethodsParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    const customer = (await StripeClient.customers.search({ query: customerQuery(req_body.tenant) })).data[0];
    // The grant check only authorizes the caller for `tenant`, so the
    // caller-supplied payment method must belong to that tenant's customer.
    const method = customer ? await StripeClient.paymentMethods.retrieve(req_body.id) : null;
    if (!method || method.customer !== customer.id) {
        return [JSON.stringify({ error: "Payment method not found for tenant" }), {
            headers: billingResponseHeaders,
            status: 404,
        }];
    }

    await StripeClient.paymentMethods.detach(req_body.id);

    const methods = (await StripeClient.customers.listPaymentMethods(customer.id)).data;
    const validMethod = methods.filter((m: { id: string }) => m.id !== req_body.id)[0];
    if (validMethod) {
        await StripeClient.customers.update(customer.id, { invoice_settings: { default_payment_method: validMethod.id } });
    }
    return [JSON.stringify({ status: "ok" }), {
        headers: billingResponseHeaders,
        status: 200,
    }];
}
