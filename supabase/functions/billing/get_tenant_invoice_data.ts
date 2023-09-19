import { customerQuery, StripeClient } from "./shared.ts";

export interface getTenantPaymentMethodsParams {
    tenant: string;
    month: string;
    type: "Usage" | "Manual";
}

const INVOICE_TYPE_KEY = "estuary.dev/invoice_type";
const BILLING_PERIOD_START_KEY = "estuary.dev/period_start";

export async function getTenantInvoiceData(
    req_body: getTenantPaymentMethodsParams,
    full_req: Request,
): Promise<ConstructorParameters<typeof Response>> {
    const customer = (await StripeClient.customers.search({ query: customerQuery(req_body.tenant) })).data[0];
    const parsed_date = new Date(req_body.month);

    const year = new Intl.DateTimeFormat("en", { year: "numeric" }).format(parsed_date);
    const month = new Intl.DateTimeFormat("en", { month: "2-digit" }).format(parsed_date);

    // We always start on the first of the month
    const metadata_date = `${year}-${month}-01`;

    if (customer) {
        const query =
            `customer:"${customer.id}" AND metadata["${INVOICE_TYPE_KEY}"]:"${req_body.type}" AND metadata["${BILLING_PERIOD_START_KEY}"]:"${metadata_date}" AND -status:"draft"`;
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
                headers: { "Content-Type": "application/json" },
                status: 200,
            }];
        }
    }

    return [JSON.stringify({ invoice: null }), {
        headers: { "Content-Type": "application/json" },
        status: 200,
    }];
}
