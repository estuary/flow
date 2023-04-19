import jsonpointer from 'https://esm.sh/jsonpointer.js';
import { returnPostgresError, compileTemplate } from '../_shared/helpers.ts';
import { corsHeaders } from '../_shared/cors.ts';
import { supabaseClient } from '../_shared/supabaseClient.ts';

interface OauthSettings {
    oauth2_client_id: string;
    oauth2_client_secret: string;
    oauth2_injected_values: any;
    oauth2_spec: any;
}

export async function accessToken(req: Record<string, any>) {
    const { state, code_verifier, config, redirect_uri, ...params } = req;

    const decodedState = JSON.parse(atob(state));
    const { connector_id } = decodedState;

    const { data, error }: { data: OauthSettings | null; error: any } = await supabaseClient
        .from('connectors')
        .select('oauth2_client_id,oauth2_client_secret,oauth2_injected_values,oauth2_spec')
        .eq('id', connector_id)
        .single();

    if (error != null) {
        returnPostgresError(error);
    }
    // TODO - check for empty data

    const { oauth2_spec, oauth2_client_id, oauth2_injected_values, oauth2_client_secret } = data as OauthSettings;

    const url = compileTemplate(
        oauth2_spec.accessTokenUrlTemplate,
        {
            redirect_uri: redirect_uri ?? 'https://dashboard.estuary.dev/oauth',
            client_id: oauth2_client_id,
            client_secret: oauth2_client_secret,
            config,
            code_verifier,
            ...oauth2_injected_values,
            ...params,
        },
        connector_id,
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
            connector_id,
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
                connector_id,
            ),
        );
    }

    const defaultContentType: Record<string, string> = Object.keys(headers).some(
        h => h.toLowerCase() == 'content-type') ? {} : { 'content-type': 'application/json' };

    const response = await fetch(url, {
        method: 'POST',
        body: body,
        headers: {
            accept: 'application/json',
            ...defaultContentType,
            ...corsHeaders,
            ...headers,
        },
    });

    const accessTokenResponseMap = oauth2_spec.accessTokenResponseMap || {};

    const responseText = await response.text();

    if (response.status >= 400) {
        console.log('access token request failed');
        console.log('request: POST ', url);
        console.log(
            'response: ',
            response.status,
            response.statusText,
            'headers: ',
            response.headers,
            'response body:',
            responseText,
        );
    }

    const responseData = JSON.parse(responseText);

    const mappedData: Record<string, any> = {};
    for (const key in accessTokenResponseMap) {
        if (accessTokenResponseMap[key].startsWith('/')) {
          mappedData[key] = jsonpointer.get(responseData, accessTokenResponseMap[key]);
        } else {
          mappedData[key] = compileTemplate(accessTokenResponseMap[key], responseData);
        }
    }

    return new Response(JSON.stringify(mappedData), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: response.status,
    });
}
