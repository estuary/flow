import { accessToken } from "./access-token.ts";
import { authURL } from "./auth-url.ts";
import { encryptConfig } from "./encrypt-config.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { OAuthUrlRefused } from "../_shared/helpers.ts";
import { createSupabaseClientWithAuthorization } from "../_shared/supabaseClient.ts";

// Separated from index.ts so that tests can drive it without serve() binding a
// port on import.
export const handleRequest = async (req: Request): Promise<Response> => {
  // This is needed if you're planning to invoke your function from a browser.
  // Remember to add the corsHeaders on the other responses as well.
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const request = await req.json();

  // `connector_config` supplies the OAuth spec inline instead of loading one
  // from the `connectors` table. It exists for the `flowctl raw oauth`
  // development workflow. Callers are expected to hold real user tokens,
  // and we require an authenticated user.
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

  try {
    if (request.operation === "auth-url") {
      return await authURL(request);
    } else if (request.operation === "access-token") {
      return await accessToken(request);
    } else if (request.operation === "encrypt-config") {
      return await encryptConfig(request);
    } else {
      return new Response(JSON.stringify({ error: "unknown_operation" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: 400,
      });
    }
  } catch (error) {
    if (error instanceof OAuthUrlRefused) {
      console.log("refusing OAuth request:", error.message);
      return new Response(JSON.stringify({ error: error.message }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: 400,
      });
    }
    throw error;
  }
};
