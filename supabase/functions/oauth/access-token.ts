import {serve} from "https://deno.land/std@0.131.0/http/server.ts"
import {createClient} from "https://esm.sh/@supabase/supabase-js";
import Handlebars from 'https://esm.sh/handlebars';

export async function accessToken(req: Record<string, any>) {
  const supabase = createClient(Deno.env.get("SUPABASE_URL")!,
                                Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!)
  const {state, config, ...params} = req;

  const decodedState = JSON.parse(atob(state));
  const {connector_id} = decodedState;

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
    redirect_uri,
    client_id : oauth2_client_id,
    client_secret : oauth2_client_secret,
    config,
    ...params
  });

  const bodyTemplate = Handlebars.compile(oauth2_spec.accessTokenBody);
  const body = bodyTemplate({
    redirect_uri,
    client_id : oauth2_client_id,
    client_secret : oauth2_client_secret,
    config,
    ...params
  });

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
}
