import { createClient } from "https://esm.sh/@supabase/supabase-js";
import Handlebars from "https://esm.sh/handlebars";
import { corsHeaders } from "../_shared/cors.ts";

export async function authURL(req: {
  connector_id: string;
  config: object;
  redirect_uri?: string;
  state?: object;
}) {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  const { connector_id, config, redirect_uri, state } = req;

  const { data, error } = await supabase
    .from("connectors")
    .select("oauth2_client_id,oauth2_spec")
    .eq("id", connector_id)
    .single();

  if (error != null) {
    return new Response(JSON.stringify(error), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 400,
    });
  }

  const { oauth2_spec, oauth2_client_id } = data;

  const finalState = btoa(
    JSON.stringify({
      ...(state ?? {}),
      verification_token: Math.random().toString(),
      connector_id,
    })
  );

  const template = Handlebars.compile(oauth2_spec.authUrlTemplate);
  const url = encodeURI(
    template({
      finalState,
      redirect_uri: redirect_uri ?? "https://dashboard.estuary.dev/oauth",
      client_id: oauth2_client_id,
      config,
    })
  );

  return new Response(JSON.stringify({ url: url, state: finalState }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

// To invoke:
// curl -i --location --request POST 'http://localhost:5431/functions/v1/'
// \
//   --header 'Authorization: Bearer
//   eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs'
//   \
//   --header 'Content-Type: application/json' \
//   --data '{"connector_id":"06:98:fc:31:e4:80:5c:00"}'
