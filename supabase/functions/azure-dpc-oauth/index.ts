// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

// Setup type definitions for built-in Supabase Runtime APIs
import "jsr:@supabase/functions-js/edge-runtime.d.ts"

Deno.serve(async (req) => {
  const u = new URL(req.url);
  const code = u.searchParams.get("code");
  if (code) {
    return new Response("data-plane-controller was successfully added to your Azure tenant, now continue with the next steps of https://docs.estuary.dev/guides/byoc/azure/")
  }

  const error = u.searchParams.get("error");
  const errorDescription = u.searchParams.get("error_description");
  return new Response("Failed to add data-plane-controller to your Azure tenant: " + error + ", " + errorDescription, { status: 400 })
})

/* To invoke locally:

  1. Run `supabase start` (see: https://supabase.com/docs/reference/cli/supabase-start)
  2. Make an HTTP request:

  curl -i --location --request POST 'http://127.0.0.1:5431/functions/v1/azure-dpc-oauth' \
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
    --header 'Content-Type: application/json' \
    --data '{"name":"Functions"}'

*/
