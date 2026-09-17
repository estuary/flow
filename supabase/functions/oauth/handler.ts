import { accessToken } from "./access-token.ts";
import { authURL } from "./auth-url.ts";
import { encryptConfig } from "./encrypt-config.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { OAuthUrlRefused } from "../_shared/helpers.ts";

// Separated from index.ts so that tests can drive it without serve() binding a
// port on import.
export const handleRequest = async (req: Request): Promise<Response> => {
  // This is needed if you're planning to invoke your function from a browser.
  // Remember to add the corsHeaders on the other responses as well.
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const request = await req.json();
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
