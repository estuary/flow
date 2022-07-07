import {serve} from "https://deno.land/std@0.131.0/http/server.ts"
import {createClient} from "https://esm.sh/@supabase/supabase-js";
import Handlebars from 'https://esm.sh/handlebars';

const supabase = createClient(Deno.env.get("SUPABASE_URL")!,
                              Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!)

console.log("Hello from Functions!")

serve(async (req) => {
  const {state, code} = await req.json()

  const decodedState = atob(state);
  const connector_id = decodedState.split('/')[1];

  const {data, error} =
      await supabase.from('connectors')
          .select('oauth2_client_id,oauth2_client_secret,oauth2_spec')
          .eq('id', connector_id)
          .single();

  if (error != null) {
    return new Response(
        JSON.stringify(error),
        {headers : {"Content-Type" : "application/json"}, status : 400});
  }

  const {oauth2_spec, oauth2_client_id, oauth2_client_secret} = data;

  const redirect_uri = "https://dashboard.estuary.dev/oauth";

  const urlTemplate = Handlebars.compile(oauth2_spec.accessTokenUrlTemplate);
  const url = urlTemplate({
    code,
    redirect_uri,
    client_id : oauth2_client_id,
    client_secret : oauth2_client_secret
  });
  console.log(url);

  const bodyTemplate = Handlebars.compile(oauth2_spec.accessTokenBody);
  const body = bodyTemplate({
    code,
    redirect_uri,
    client_id : oauth2_client_id,
    client_secret : oauth2_client_secret
  });

  console.log(body);

  const response = await fetch(url, {
    method : "POST",
    body : body,
    headers : {
      accept : "application/json",
      "content-type" : "application/json",
    }
  });

  return new Response(
      JSON.stringify(await response.json()),
      {
        headers : {"Content-Type" : "application/json"},
        status : response.status
      },
  )
})

    // To invoke:
    // curl -i --location --request POST 'http://localhost:5431/functions/v1/'
    // \
    //   --header 'Authorization: Bearer
    //   eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs'
    //   \
    //   --header 'Content-Type: application/json' \
    //   '{"state":"MC44ODIxNjczODc5NjE2Nzg1LzA2Ojk4OmZjOjMxOmU0OjgwOjVjOjAw","code":
    //   "4/0AX4XfWhPiO7SNUn0Tl7MHya8oDmzYSieXKeqOkYb3ckZ1zpRJ4odKI0BHXAHvFXQrP0_3A"}'
