import {serve} from "https://deno.land/std@0.131.0/http/server.ts"
import {createClient} from "https://esm.sh/@supabase/supabase-js";
import Handlebars from 'https://esm.sh/handlebars';

const supabase = createClient(Deno.env.get("SUPABASE_URL")!,
                              Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!)

console.log("Hello from Functions!")

serve(async (req) => {
  const {connector_id} = await req.json()

  const {data, error} = await supabase.from('connectors')
                            .select('oauth2_client_id,oauth2_spec')
                            .eq('id', connector_id)
                            .single();

  if (error != null) {
    return new Response(
        JSON.stringify(error),
        {headers : {"Content-Type" : "application/json"}, status : 400});
  }

  const {oauth2_spec, oauth2_client_id} = data;

  // TODO: let frontend handle state
  const state = btoa(`${Math.random().toString()}/${connector_id}`);
  const redirect_uri = "https://dashboard.estuary.dev/oauth";

  const template = Handlebars.compile(oauth2_spec.authUrlTemplate);
  const url = template({state, redirect_uri, client_id : oauth2_client_id});

  return new Response(
      JSON.stringify({"url" : url}),
      {headers : {"Content-Type" : "application/json"}},
  )
})

    // To invoke:
    // curl -i --location --request POST 'http://localhost:54321/functions/v1/'
    // \
    //   --header 'Authorization: Bearer
    //   eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs'
    //   \
    //   --header 'Content-Type: application/json' \
    //   --data '{"connector_id":"06:98:fc:31:e4:80:5c:00"}'
