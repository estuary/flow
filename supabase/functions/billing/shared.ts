import Stripe from "https://esm.sh/v114/stripe@12.1.1";
// Dumb hack to fix deno not picking up the types for some reason
import type StripeType from "https://esm.sh/v116/stripe@12.1.1/types/index.d.ts";

const STRIPE_API = Deno.env.get("STRIPE_API_KEY");
if (!STRIPE_API) {
    throw new Error("Unable to locate STRIPE_API_KEY environment variable");
}
export const StripeClient: StripeType = new Stripe(STRIPE_API, { apiVersion: "2022-11-15" });

export const TENANT_METADATA_KEY = "estuary.dev/tenant_name";
export const customerQuery = (tenant: string) => `metadata["${TENANT_METADATA_KEY}"]:"${tenant}"`;
