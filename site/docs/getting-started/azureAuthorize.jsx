import React from "react";

const SETTINGS = {
    appId: "42cb0c6c-dab0-411f-9c21-16d5a2b1b025",
    redirectUri: "https://eyrcnmuzzyriypdajwdk.supabase.co/functions/v1/azure-dpc-oauth",
    resourceId: 'https://storage.azure.com',
    responseType: 'code',
    stateKey: 'state',
    storageKey: 'docs_est:azure_oauth_state'
}

const generateAuthorizeUrl = (theirTenant) => {
    const state = crypto.randomUUID()
    sessionStorage.setItem(SETTINGS.storageKey, state);

    const params = new URLSearchParams({
      client_id: SETTINGS.appId,
      redirect_uri: SETTINGS.redirectUri,
      resource_id: SETTINGS.resourceId,
      response_type: SETTINGS.responseType,
      [SETTINGS.stateKey]: state,
    });

    return `https://login.microsoftonline.com/${encodeURIComponent(tenant)}/oauth2/authorize?${params}`
};

export const AzureAuthorizeComponent = () => {
    const [tenant, setTenant] = React.useState("");

    // Try to get the auth code but first ensure the states match
    //  if there is an issue this will fail silently (Q4 2025)
    const authCode = React.useMemo(() => {
        const params = new URLSearchParams(window.location.search);
        const code = params.get(SETTINGS.responseType);
        const returnedState = params.get(SETTINGS.stateKey);
        const storedState = sessionStorage.getItem(SETTINGS.storageKey);

        if (code) {
            sessionStorage.removeItem(SETTINGS.storageKey);

            if (!returnedState || !storedState || returnedState !== storedState) {
                return null;
            }

            return code;
        }

        return null;
    }, []);

    if (authCode) {
        return (
            <span style={{ color: "green" }}>
                You have successfully added the application to your tenant
            </span>
        );
    } else {
        return (
            <>
                <span>
                    Input your <b>Tenant ID</b> into the field below. Then click the <b>Authorize</b> button
                    to begin the OAuth process that will add our application to your tenant:
                </span>
                <br />
                <br />
                <center>
                    <input
                        placeholder="Your Azure Tenant ID"
                        value={tenant}
                        onChange={(e) => setTenant(e.target.value)}
                        style={{
                            padding: 8
                        }}
                    />
                    <a
                        style={{
                            marginLeft: 8,
                            padding: 10,
                            color: "white",
                            backgroundColor: tenant.length < 1 ? "lightgray" : "#3B43FE",
                            fontWeight: tenant.length < 1 ? "inherit" : "bold",
                            borderRadius: 20
                        }}
                        href={
                            tenant.length > 0 ? generateAuthorizeUrl(tenant) : null
                        }
                    >
                        Authorize
                    </a>
                </center>
            </>
        );
    }
};
