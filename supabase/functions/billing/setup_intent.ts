import { SupabaseClient, User } from "https://esm.sh/@supabase/supabase-js@2.0.5";
import { billingResponseHeaders, customerQuery, StripeClient, TENANT_METADATA_KEY } from "./shared.ts";

async function findOrCreateCustomer(tenant: string, user: User) {
    if (!user.email) {
        throw new Error("Missing user email address");
    }
    const query = customerQuery(tenant);
    const existing = await StripeClient.customers.search({
        query,
    });
    if (existing?.data?.length === 1) {
        console.log(`Found existing customer, reusing `);
        return existing.data[0];
    } else if (existing?.data?.length || 0 > 1) {
        console.log(`Found existing customer, reusing `);
        // Should we bail?
        return existing.data[0];
    } else {
        console.log(`Unable to find customer, creating new`);
        const customer = await StripeClient.customers.create({
            email: user.email,
            name: tenant,
            description: `Represents the billing entity for Flow tenant '${tenant}'`,

            metadata: {
                [TENANT_METADATA_KEY]: tenant,
                "created_by_user_email": user.email,
                "created_by_user_name": user.user_metadata.name,
            },
        });

        return customer;
    }
}

export interface SetupIntentRequest {
    tenant: string;
}

export async function setupIntent(
    req_body: SetupIntentRequest,
    full_req: Request,
    supabase_client: SupabaseClient,
): Promise<ConstructorParameters<typeof Response>> {
    // Now we can get the session or user object
    const {
        data: { user },
    } = await supabase_client.auth.getUser();

    if (!user) {
        return [JSON.stringify({ error: "User not found" }), {
            headers: billingResponseHeaders,
            status: 400,
        }];
    }
    const customer = await findOrCreateCustomer(req_body.tenant, user);

    const intent = await StripeClient.setupIntents.create({
        customer: customer.id,
        description: "Store your payment details",
        usage: "off_session",
        automatic_payment_methods: {
            enabled: true
        }
    });

    return [JSON.stringify({ intent_secret: intent.client_secret }), {
        headers: billingResponseHeaders,
        status: 200,
    }];
}
