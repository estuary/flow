import { billingResponseHeaders, customerQuery, StripeClient } from "./shared.ts";

export interface SetTenantPrimaryPaymentMethodParams {
    tenant: string;
    id: string;
}

export async function setTenantPrimaryPaymentMethod(
    req_body: SetTenantPrimaryPaymentMethodParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    const customer = (await StripeClient.customers.search({ query: customerQuery(req_body.tenant) })).data[0];
    if (customer) {
        await StripeClient.customers.update(customer.id, { invoice_settings: { default_payment_method: req_body.id } });

        return [JSON.stringify({ status: "ok" }), {
            headers: billingResponseHeaders,
            status: 200,
        }];
    } else {
        return [JSON.stringify({ payment_methods: [], primary: null }), {
            headers: billingResponseHeaders,
            status: 200,
        }];
    }
}
