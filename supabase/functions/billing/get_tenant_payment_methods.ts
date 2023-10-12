import { customerQuery, StripeClient } from "./shared.ts";

export interface getTenantPaymentMethodsParams {
    tenant: string;
}

export async function getTenantPaymentMethods(
    req_body: getTenantPaymentMethodsParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    const customer = (await StripeClient.customers.search({ query: customerQuery(req_body.tenant) })).data[0];
    if (customer) {
        const methods = (await StripeClient.customers.listPaymentMethods(customer.id)).data;

        return [JSON.stringify({ 
            payment_methods: methods,
            primary: customer.invoice_settings.default_payment_method,
            tenant: req_body.tenant
        }), {
            headers: { "Content-Type": "application/json" },
            status: 200,
        }];
    } else {
        return [JSON.stringify({ 
            payment_methods: [],
            primary: null,
            tenant: req_body.tenant
        }), {
            headers: { "Content-Type": "application/json" },
            status: 200,
        }];
    }
}
