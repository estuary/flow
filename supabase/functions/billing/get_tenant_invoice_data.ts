import { billingResponseHeaders, customerQuery, StripeClient } from "./shared.ts";

export interface getTenantPaymentMethodsParams {
    tenant: string;
    date_start: string;
    date_end: string;
    type: "Usage" | "Manual";
}

const INVOICE_TYPE_KEY = "estuary.dev/invoice_type";
const BILLING_PERIOD_START_KEY = "estuary.dev/period_start";
const BILLING_PERIOD_END_KEY = "estuary.dev/period_end";

function formatDate(date: string): string {
    const parsed_date = new Date(date);

    const year = new Intl.DateTimeFormat("en", { year: "numeric" }).format(parsed_date);
    const month = new Intl.DateTimeFormat("en", { month: "2-digit" }).format(parsed_date);
    const day = new Intl.DateTimeFormat("en", { day: "2-digit" }).format(parsed_date);

    return `${year}-${month}-${day}`;
}

export async function getTenantInvoice(
    req_body: getTenantPaymentMethodsParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    const customer = (await StripeClient.customers.search({ query: customerQuery(req_body.tenant) })).data[0];
    const period_start = formatDate(req_body.date_start);
    const period_end = formatDate(req_body.date_end);

    if (customer) {
        const query =
            `customer:"${customer.id}" AND metadata["${INVOICE_TYPE_KEY}"]:"${req_body.type}" AND metadata["${BILLING_PERIOD_START_KEY}"]:"${period_start}" AND metadata["${BILLING_PERIOD_END_KEY}"]:"${period_end}" AND -status:"draft"`;
        const resp = await StripeClient.invoices.search({
            query,
        });

        if (resp.data[0]) {
            const limited_invoice = {
                id: resp.data[0].id,
                amount_due: resp.data[0].amount_due,
                invoice_pdf: resp.data[0].invoice_pdf,
                hosted_invoice_url: resp.data[0].hosted_invoice_url,
                status: resp.data[0].status,
            };

            return [JSON.stringify({ invoice: limited_invoice }), {
                headers: billingResponseHeaders,
                status: 200,
            }];
        }
    }

    return [JSON.stringify({ invoice: null }), {
        headers: billingResponseHeaders,
        status: 200,
    }];
}
