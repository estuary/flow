// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

import { serve } from "https://deno.land/std@0.131.0/http/server.ts"
import { supabaseClient } from "../_shared/supabaseClient.ts";

console.log("GitHub webhooks functions started.")

serve(async (req) => {
  const { action, ...body } = await req.json()

  const package_url: string = body.package.package_version.package_url;
  const [image_name, image_tag] = package_url.split(':');
  const { data: connector, error }: { data: { id: string, image_name: string } | null, error: any } = await supabaseClient
    .from("connectors")
    .select("id,image_name")
    .eq("image_name", image_name)
    .single();

  if (error != null) {
    console.error("error finding connector", error);
    return new Response(
      '{"error": "could not find connector"}',
      { headers: { "Content-Type": "application/json" } },
    );
  }

  const { data: connector_tag, error: err }: { data: object | null, error: any } = await supabaseClient
    .from("connector_tags")
    .update({ job_status: {"type":"queued"}})
    .match({ "connector_id": connector?.id, "image_tag": `:${image_tag}` });

  if (err != null) {
    console.error("error updating connector_tags", err);
  }

  return new Response(
    '{}',
    { headers: { "Content-Type": "application/json" } },
  )
})

// To invoke:
// curl -i --location --request POST 'http://localhost:54321/functions/v1/' \
//   --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs' \
//   --header 'Content-Type: application/json' \
//   --data '{
//   "action":"published", "package": {
//    "package_version": {
//      "package_url": "ghcr.io/estuary/source-hello-world:v1"
//      }
//    }
//  }'
