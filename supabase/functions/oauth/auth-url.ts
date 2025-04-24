import { createClient } from "https://esm.sh/@supabase/supabase-js?target=deno";
import Mustache from "https://esm.sh/mustache?target=deno";
import { corsHeaders } from "../_shared/cors.ts";
import { compileTemplate, returnPostgresError } from "../_shared/helpers.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

import { sha256 } from "https://denopkg.com/chiefbiiko/sha256@v1.0.0/mod.ts";

const generateUniqueRandomKey = (size: number = 40) => {
    const validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let array = new Uint8Array(size) as any;
    window.crypto.getRandomValues(array);
    array = array.map((x: number) => validChars.codePointAt(x % validChars.length));
    return String.fromCharCode.apply(null, array);
};

// map a standard base64 encoding to a url-safe encoding
// see https://www.oauth.com/oauth2-servers/pkce/authorization-request/
const base64URLSafe = (str: string) =>
    str.replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/\=+/, "");

interface OauthSettings {
    oauth2_client_id: string;
    oauth2_spec: any;
}

export async function authURL(req: { connector_id?: string; connector_config?: OauthSettings; config: object; redirect_uri?: string; state?: object }) {
    const { connector_id, config, redirect_uri, state, connector_config } = req;

    let data: OauthSettings;

    if (connector_id) {
        const { data: output_data, error }: { data: OauthSettings | null; error: any } = await supabaseClient
            .from("connectors")
            .select("oauth2_client_id,oauth2_spec")
            .eq("id", connector_id)
            .single();

        if (error != null) {
            return returnPostgresError(error);
        }
        // TODO - check for empty data
        data = output_data;
    } else if (connector_config) {
        data = connector_config;
    } else {
        return returnPostgresError("Invalid input");
    }

    const { oauth2_spec, oauth2_client_id } = data as OauthSettings;

    const finalState = btoa(
        JSON.stringify({
            ...(state ?? {}),
            verification_token: generateUniqueRandomKey(),
            connector_id,
        }),
    );

    // See https://www.oauth.com/oauth2-servers/pkce/authorization-request/
    const codeVerifier = generateUniqueRandomKey(50);
    const codeChallenge = base64URLSafe(sha256(codeVerifier, "utf8", "base64") as string);
    const codeChallengeMethod = "S256";

    const url = compileTemplate(
        oauth2_spec.authUrlTemplate,
        {
            state: finalState,
            redirect_uri: redirect_uri ?? "https://dashboard.estuary.dev/oauth",
            client_id: oauth2_client_id,
            config,
            code_challenge: codeChallenge,
            code_challenge_method: codeChallengeMethod,
        },
    );

    return new Response(JSON.stringify({ url: url, state: finalState, code_verifier: codeVerifier }), {
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
