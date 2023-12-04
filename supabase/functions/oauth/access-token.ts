import jsonpointer from "https://esm.sh/jsonpointer.js";
import { compileTemplate, returnPostgresError } from "../_shared/helpers.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { supabaseClient } from "../_shared/supabaseClient.ts";

interface OauthSettings {
    oauth2_client_id: string;
    oauth2_client_secret: string;
    oauth2_injected_values: any;
    oauth2_spec: any;
}

export async function accessToken(req: Record<string, any>) {
    const { state, code_challenge: incoming_code_challenge, config, redirect_uri, connector_config, ...params } = req;

    const { data: loadedFlow, error } = await supabaseClient
        .from("oauth_flows")
        .select("connector_id,code_verifier,code_challenge")
        .eq("state", state)
        .single();

    if (error != null) {
        returnPostgresError(error);
    }

    if (incoming_code_challenge !== loadedFlow?.code_challenge) {
        return new Response(JSON.stringify({ error: "Code challenge mismatch" }), {
            headers: {
                ...corsHeaders,
                "Content-Type": "application/json",
            },
            status: 403,
        });
    }

    const { connector_id, code_verifier } = loadedFlow;

    let data: OauthSettings;

    if (connector_config) {
        data = connector_config;
    } else {
        const { data: output_data, error }: { data: OauthSettings | null; error: any } = await supabaseClient
            .from("connectors")
            .select("oauth2_client_id,oauth2_client_secret,oauth2_injected_values,oauth2_spec")
            .eq("id", connector_id)
            .single();

        if (error != null) {
            returnPostgresError(error);
        }
        // TODO - check for empty data
        data = output_data;
    }

    const { oauth2_spec, oauth2_client_id, oauth2_injected_values, oauth2_client_secret } = data as OauthSettings;

    const url = compileTemplate(
        oauth2_spec.accessTokenUrlTemplate,
        {
            redirect_uri: redirect_uri ?? "https://dashboard.estuary.dev/oauth",
            client_id: oauth2_client_id,
            client_secret: oauth2_client_secret,
            config,
            code_verifier,
            ...oauth2_injected_values,
            ...params,
        },
    );

    let body = null;
    if (oauth2_spec.accessTokenBody) {
        body = compileTemplate(
            oauth2_spec.accessTokenBody,
            {
                redirect_uri,
                client_id: oauth2_client_id,
                client_secret: oauth2_client_secret,
                config,
                code_verifier,
                ...oauth2_injected_values,
                ...params,
            },
        );
    }

    let headers = {};
    if (oauth2_spec.accessTokenHeaders) {
        headers = JSON.parse(
            compileTemplate(
                JSON.stringify(oauth2_spec.accessTokenHeaders),
                {
                    redirect_uri,
                    client_id: oauth2_client_id,
                    client_secret: oauth2_client_secret,
                    config,
                    code_verifier,
                    ...oauth2_injected_values,
                    ...params,
                },
            ),
        );
    }

    const defaultContentType: Record<string, string> = Object.keys(headers).some(
            (h) => h.toLowerCase() == "content-type",
        )
        ? {}
        : { "content-type": "application/json" };

    console.log(`Request URL: ${url}. Request body: ${JSON.stringify(body, null, 4)}. Request headers: ${JSON.stringify(headers, null, 4)}`);

    const response = await fetch(url, {
        method: "POST",
        body: body,
        headers: {
            accept: "application/json",
            ...defaultContentType,
            ...corsHeaders,
            ...headers,
        },
    });

    const accessTokenResponseMap = oauth2_spec.accessTokenResponseMap || {};

    const responseText = await response.text();

    if (response.status >= 400) {
        console.log("access token request failed");
        console.log("request: POST ", url);
        console.log(
            "response: ",
            response.status,
            response.statusText,
            "headers: ",
            response.headers,
            "response body:",
            responseText,
        );
    }

    const responseData = JSON.parse(responseText);

    const mappedData: Record<string, any> = {};
    for (const key in accessTokenResponseMap) {
        if (accessTokenResponseMap[key].startsWith("/")) {
            mappedData[key] = jsonpointer.get(responseData, accessTokenResponseMap[key]);
        } else {
            mappedData[key] = compileTemplate(accessTokenResponseMap[key], responseData);
        }
    }

    return new Response(JSON.stringify(mappedData), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: response.status,
    });
}
