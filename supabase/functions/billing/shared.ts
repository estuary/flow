import Stripe from "npm:stripe@12.2.0";
import { corsHeaders } from '../_shared/cors.ts';

const STRIPE_API = Deno.env.get("STRIPE_API_KEY");
if (!STRIPE_API) {
    throw new Error("Unable to locate STRIPE_API_KEY environment variable");
}
// deno-lint-ignore no-explicit-any
export const StripeClient = new Stripe(STRIPE_API, { apiVersion: "2022-11-15" }) as any;

export const TENANT_METADATA_KEY = "estuary.dev/tenant_name";
export const customerQuery = (tenant: string) => `metadata["${TENANT_METADATA_KEY}"]:"${tenant}"`;

export const billingResponseHeaders = { "Content-Type": "application/json", ...corsHeaders };