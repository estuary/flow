import _ from "npm:lodash";

import { corsHeaders } from "../_shared/cors.ts";
import { returnPostgresError } from "../_shared/helpers.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

const config_encryption_url = () => {
  const env = Deno.env.get('CONFIG_ENCRYPTION_URL');
  // A more principled approach would be to require that this url is always provided by the
  // env var. But that would require using a supabase secret to set the value in prod,
  // and _that_ seemed like a whole ordeal that I don't have time for right now.
  if (env) {
    return env
  } else {
    return "https://config-encryption.estuary.dev/v1/encrypt-config"
  }
}

const CREDENTIALS_KEY = "credentials";

const CLIENT_CREDS_INJECTION = "_injectedDuringEncryption_";

interface OauthSettings {
  oauth2_client_id: string,
  oauth2_client_secret: string,
  oauth2_injected_values: any
}

interface ConnectorTagsResponse {
  endpoint_spec_schema: any
}

// Note: it's important that we only ever use trusted `endpoint_spec_schemas` here,
// as that is how we determine which fields need to be encrypted.
export async function encryptConfig(req: Record<string, any>) {
  const { connector_id, connector_tag_id, config } = req;

  const { data, error }: { data: OauthSettings | null, error: any } = await supabaseClient
    .from("connectors")
    .select("oauth2_client_id,oauth2_client_secret,oauth2_injected_values")
    .eq("id", connector_id)
    .single();

  if (error != null) {
    returnPostgresError(error);
  }
  // TODO - check for empty data

  const { oauth2_client_id, oauth2_client_secret, oauth2_injected_values } = data as OauthSettings;

  if (
    config?.[CREDENTIALS_KEY]?.["client_id"] === CLIENT_CREDS_INJECTION &&
    config?.[CREDENTIALS_KEY]?.["client_secret"] === CLIENT_CREDS_INJECTION
  ) {
    config[CREDENTIALS_KEY]["client_id"] = oauth2_client_id;
    config[CREDENTIALS_KEY]["client_secret"] = oauth2_client_secret;
    Object.assign(config[CREDENTIALS_KEY], oauth2_injected_values);
  }

  const { data: connectorTagData, error: connectorTagError } =
    await supabaseClient
      .from("connector_tags")
      .select("endpoint_spec_schema")
      .eq("id", connector_tag_id)
      .single();

  if (connectorTagError != null) {
    returnPostgresError(error);
  }

  const { endpoint_spec_schema } = connectorTagData as ConnectorTagsResponse;

  const response = await fetch(config_encryption_url(), {
    method: "POST",
    body: JSON.stringify({
      config,
      schema: endpoint_spec_schema,
    }),
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
  });

  if (response.status >= 400) {
    console.error(await response.text());

    return new Response(
      JSON.stringify({
        error: {
          code: "encryption_failure",
          message: `Encryption failed.`,
          description: `Failed to encrypt the endpoint specification.`,
        },
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: response.status,
      }
    );
  }

  const responseData = JSON.stringify(await response.json());

  // If we can find client_id or client_secret in plaintext in the response,
  // it's not secure to return this response!
  if (
    (typeof oauth2_client_id === "string" &&
      oauth2_client_id.length > 0 &&
      responseData.includes(oauth2_client_id)) ||
    (typeof oauth2_client_secret === "string" &&
      oauth2_client_secret.length > 0 &&
      responseData.includes(oauth2_client_secret)) ||
    (typeof oauth2_injected_values === "object" &&
      oauth2_injected_values !== null &&
      _.some(_.values(oauth2_injected_values), (value: string) => responseData.includes(value)))
  ) {
    return new Response(
      JSON.stringify({
        error: {
          code: "exposed_secret",
          message: `Request denied: "client id", "client secret" or some other injected secrets could have been leaked.`,
          description: `client_id, client_secret or some other injected secrets were not encrypted as part of this request.
Make sure that they are marked with secret: true in the endpoint spec schema`,
        },
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: response.status,
      }
    );
  }

  return new Response(responseData, {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: response.status,
  });
}
