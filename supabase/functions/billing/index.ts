import { serve } from "https://deno.land/std@0.184.0/http/server.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { setupIntent } from "./setup_intent.ts";
import { getTenantPaymentMethods } from "./get_tenant_payment_methods.ts";
import { deleteTenantPaymentMethod } from "./delete_tenant_payment_method.ts";
import { setTenantPrimaryPaymentMethod } from "./set_tenant_primary_payment_method.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.0.5";

serve(async (req) => {
    let res: ConstructorParameters<typeof Response> = [null, {}];
    try {
        // This is needed if you're planning to invoke your function from a browser.
        // Remember to add the corsHeaders on the other responses as well.
        if (req.method === "OPTIONS") {
            res = ["ok", { status: 200 }];
        } else {
            const request = await req.json();

            const requested_tenant = request.tenant;
            // Create a Supabase client with the Auth context of the logged in user.
            const supabaseClient = createClient(
                // Supabase API URL - env var exported by default.
                Deno.env.get("SUPABASE_URL") ?? "",
                // Supabase API ANON KEY - env var exported by default.
                Deno.env.get("SUPABASE_ANON_KEY") ?? "",
                // Create client with Auth context of the user that called the function.
                // This way your row-level-security (RLS) policies are applied.
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

            const grants = await supabaseClient.from("combined_grants_ext").select("*").eq("capability", "admin").eq("user_id", user.id);

            if (!(grants.data ?? []).find((grant) => grant.object_role === requested_tenant)) {
                res = [JSON.stringify({ error: `Not authorized to requested grant` }), {
                    headers: { "Content-Type": "application/json" },
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
                } else {
                    res = [JSON.stringify({ error: "unknown_operation" }), {
                        headers: { "Content-Type": "application/json" },
                        status: 400,
                    }];
                }
            }
        }
    } catch (e) {
        res = [JSON.stringify({ error: e.message }), {
            headers: { "Content-Type": "application/json" },
            status: 400,
        }];
    }

    res[1] = { ...res[1], headers: { ...res[1]?.headers || {}, ...corsHeaders } };

    console.log(JSON.stringify(res, null, 4));

    return new Response(...res);
});
