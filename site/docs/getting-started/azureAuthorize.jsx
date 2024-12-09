import React from "react";

export const AzureAuthorizeComponent = () => {
    const ourAppId = "42cb0c6c-dab0-411f-9c21-16d5a2b1b025";
    const redirectUri = window.location.href;
    const resourceId = "https://storage.azure.com";

    const generateAuthorizeUrl = (theirTenant) =>
        `https://login.microsoftonline.com/${theirTenant}/oauth2/authorize?client_id=${ourAppId}&response_type=code&redirect_uri=${encodeURIComponent(
            redirectUri
        )}&resource_id=${encodeURIComponent(resourceId)}`;

    const [tenant, setTenant] = React.useState("");

    const authCode = React.useMemo(() => {
        return new URLSearchParams(window.location.search.slice(1)).get("code");
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
                    Input your <b>Tenant ID</b> and follow the prompts to add
                    our application to your tenant:
                </span>
                <br />
                <br />
                <input
                    placeholder="Tenant ID"
                    value={tenant}
                    onChange={(e) => setTenant(e.target.value)}
                />
                <a
                    style={{
                        marginLeft: 8,
                        color: tenant.length < 1 ? "inherit" : undefined,
                    }}
                    href={
                        tenant.length > 0 ? generateAuthorizeUrl(tenant) : null
                    }
                >
                    Authorize
                </a>
            </>
        );
    }
};
