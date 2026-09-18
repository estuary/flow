import { serve } from "https://deno.land/std@0.184.0/http/server.ts";

import { accessToken } from "./access-token.ts";
import { authURL } from "./auth-url.ts";
import { encryptConfig } from "./encrypt-config.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { createSupabaseClientWithAuthorization } from "../_shared/supabaseClient.ts";

serve(async (req) => {
  // This is needed if you're planning to invoke your function from a browser.
  // Remember to add the corsHeaders on the other responses as well.
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const request = await req.json();

  // `connector_config` supplies the OAuth spec inline instead of loading one
  // from the `connectors` table. It exists for the `flowctl raw oauth`
  // development workflow, whose callers hold real user tokens, so require an
  // authenticated user rather than accepting the anonymous key that every
  // request to this function already carries.
  if (request.connector_config) {
    const authHeader = req.headers.get("Authorization");
    if (!authHeader) {
      return new Response(JSON.stringify({ error: "Missing Authorization header" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: 401,
      });
    }
    const userClient = createSupabaseClientWithAuthorization(authHeader);
    const {
      data: { user },
    } = await userClient.auth.getUser();

    if (!user) {
      return new Response(JSON.stringify({ error: "User not found" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: 401,
      });
    }
  }

  if (request.operation === "auth-url") {
    return authURL(request);
  } else if (request.operation === "access-token") {
    return accessToken(request);
  } else if (request.operation === "encrypt-config") {
    return encryptConfig(request);
  } else {
    return new Response(JSON.stringify({ error: "unknown_operation" }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 400,
    });
  }
});
