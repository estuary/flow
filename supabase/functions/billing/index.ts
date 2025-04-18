import { serve } from "https://deno.land/std@0.184.0/http/server.ts";
import { billingResponseHeaders } from "./shared.ts";
import { setupIntent } from "./setup_intent.ts";
import { getTenantPaymentMethods } from "./get_tenant_payment_methods.ts";
import { deleteTenantPaymentMethod } from "./delete_tenant_payment_method.ts";
import { setTenantPrimaryPaymentMethod } from "./set_tenant_primary_payment_method.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.0.5";
import { getTenantInvoice } from "./get_tenant_invoice_data.ts";

// Now that the supabase CLI supports multiple edge functions,
// we should refactor this into individual functions instead
// of multiplexing endpoints via the request body
serve(async (req) => {
    let res: ConstructorParameters<typeof Response> = [null, {}];
    try {
        if (req.method === "OPTIONS") {
            res = ["ok", { headers: billingResponseHeaders, status: 200 }];
        } else {
            const request = await req.json();

            const requested_tenant = request.tenant;
            // Create a Supabase client with the Auth context of the logged in user.
            // This is required in order to get the user's name and email address

            const supabaseClient = createClient(
                Deno.env.get("SUPABASE_URL") ?? "",
                Deno.env.get("SUPABASE_ANON_KEY") ?? "",
                {
                    global: {
                        headers: { Authorization: req.headers.get("Authorization")! },
                    },
                },
            );

            const {
                data: { user },
            } = await supabaseClient.auth.getUser();

            if (!user) {
                throw new Error("User not found");
            }

            const grants = await supabaseClient.from("combined_grants_ext").select("*").eq("capability", "admin").eq("object_role", requested_tenant);

            if (!grants || !grants.data || grants.data.length === 0) {
                console.warn('billing:grants:missing', {
                    grants
                });

                res = [JSON.stringify({ error: `Not authorized to requested grant` }), {
                    headers: billingResponseHeaders,
                    status: 401,
                }];
            } else {
                if (request.operation === "setup-intent") {
                    res = await setupIntent(request, req, supabaseClient);
                } else if (request.operation === "get-tenant-payment-methods") {
                    res = await getTenantPaymentMethods(request, req);
                } else if (request.operation === "delete-tenant-payment-method") {
                    res = await deleteTenantPaymentMethod(request, req);
                } else if (request.operation === "set-tenant-primary-payment-method") {
                    res = await setTenantPrimaryPaymentMethod(request, req);
                } else if (request.operation === "get-tenant-invoice") {
                    res = await getTenantInvoice(request, req);
                } else {
                    res = [JSON.stringify({ error: "unknown_operation" }), {
                        headers: billingResponseHeaders,
                        status: 400,
                    }];
                }
            }
        }
    } catch (e) {
        res = [JSON.stringify({ error: e.message }), {
            headers: billingResponseHeaders,
            status: 400,
        }];
    }

    res[1] = { ...res[1], headers: { ...res[1]?.headers || billingResponseHeaders } };

    return new Response(...res);
});
