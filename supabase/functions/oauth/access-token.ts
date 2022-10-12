import { serve } from "https://deno.land/std@0.131.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js";
import Handlebars from "https://esm.sh/handlebars";
import jsonpointer from "https://esm.sh/jsonpointer.js";
import { returnPostgresError, handlebarsHelpers } from "../_shared/helpers.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

export async function accessToken(req: Record<string, any>) {
  const { state, config, redirect_uri, ...params } = req;
  Handlebars.registerHelper(handlebarsHelpers);

  const decodedState = JSON.parse(atob(state));
  const { connector_id } = decodedState;

  const { data, error } = await supabaseClient
    .from("connectors")
    .select("oauth2_client_id,oauth2_client_secret,oauth2_injected_values,oauth2_spec")
    .eq("id", connector_id)
    .single();

  if (error != null) {
    returnPostgresError(error);
  }

  const { oauth2_spec, oauth2_client_id, oauth2_injected_values, oauth2_client_secret } = data;

  const urlTemplate = Handlebars.compile(oauth2_spec.accessTokenUrlTemplate);
  const url = urlTemplate({
    redirect_uri: redirect_uri ?? "https://dashboard.estuary.dev/oauth",
    client_id: oauth2_client_id,
    client_secret: oauth2_client_secret,
    config,
    ...oauth2_injected_values,
    ...params,
  });

  let body = null;
  if (oauth2_spec.accessTokenBody) {
    const bodyTemplate = Handlebars.compile(
      oauth2_spec.accessTokenBody
    );
    body = bodyTemplate({
      redirect_uri,
      client_id: oauth2_client_id,
      client_secret: oauth2_client_secret,
      config,
      ...oauth2_injected_values,
      ...params,
    });
  }

  let headers = {};
  if (oauth2_spec.accessTokenHeaders) {
    const headersTemplate = Handlebars.compile(
      JSON.stringify(oauth2_spec.accessTokenHeaders)
    );
    headers = JSON.parse(
      headersTemplate({
        redirect_uri,
        client_id: oauth2_client_id,
        client_secret: oauth2_client_secret,
        config,
        ...oauth2_injected_values,
        ...params,
      })
    );
  }

  const response = await fetch(url, {
    method: "POST",
    body: body,
    headers: {
      accept: "application/json",
      "content-type": "application/json",
      ...corsHeaders,
      ...headers,
    },
  });

  const accessTokenResponseMap = oauth2_spec.accessTokenResponseMap || {};

  const responseText = await response.text();

  if (response.status >= 400) {
    console.log("access token request failed");
    console.log("request: POST ", url);
    console.log("response: ", response.status, response.statusText, "headers: ", response.headers, "response body:", responseText);
  }

  let responseData = JSON.parse(responseText);

  let mappedData: Record<string, any> = {};
  for (const key in accessTokenResponseMap) {
    mappedData[key] = jsonpointer.get(
      responseData,
      accessTokenResponseMap[key]
    );
  }

  return new Response(JSON.stringify(mappedData), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: response.status,
  });
}
