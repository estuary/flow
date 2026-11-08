import { afterAll, beforeAll, beforeEach, describe, it } from "jsr:@std/testing/bdd";
import { expect } from "jsr:@std/expect";

// supabaseClient.ts builds its client at module load, before the import below.
Deno.env.set("SUPABASE_URL", "http://localhost:9999");
Deno.env.set("SUPABASE_SERVICE_ROLE_KEY", "dummy-service-role-key");

const { accessToken } = await import("../oauth/access-token.ts");
const { OAuthUrlRefused } = await import("../_shared/helpers.ts");

const CLIENT_SECRET = "platform-client-secret";

const connectorConfig = {
    oauth2_client_id: "platform-client-id",
    oauth2_client_secret: CLIENT_SECRET,
    oauth2_injected_values: null,
    oauth2_spec: {
        provider: "zendesk",
        accessTokenUrlTemplate: "https://{{{ config.subdomain }}}.zendesk.com/oauth/tokens",
        accessTokenBody:
            '{"grant_type": "authorization_code", "code": "{{{ code }}}", "client_id": "{{{ client_id }}}", "client_secret": "{{{ client_secret }}}", "redirect_uri": "{{{ redirect_uri }}}", "scope": "read"}',
        accessTokenHeaders: { "Content-Type": "application/json" },
    },
};

const state = btoa(JSON.stringify({ connector_id: "00:00:00:00:00:00:00:01" }));

describe("accessToken", () => {
    const realFetch = globalThis.fetch;
    let sent: { url: string; body: string }[] = [];

    beforeAll(() => {
        globalThis.fetch = ((url: any, init: any) => {
            sent.push({ url: String(url), body: String(init?.body ?? "") });
            return Promise.resolve(new Response('{"access_token":"a-token"}', { status: 200 }));
        }) as any;
    });
    afterAll(() => {
        globalThis.fetch = realFetch;
    });
    beforeEach(() => {
        sent = [];
    });

    const exchange = (subdomain: string) =>
        accessToken({
            operation: "access-token",
            state,
            code: "an-authorization-code",
            config: { subdomain },
            connector_config: connectorConfig,
        });

    it("exchanges against the provider for a benign subdomain", async () => {
        await exchange("acmeco");

        expect(sent.length).toBe(1);
        expect(new URL(sent[0].url).host).toBe("acmeco.zendesk.com");
        expect(sent[0].body).toContain(CLIENT_SECRET);
    });

    it("refuses an escaped host without making any request", async () => {
        await expect(exchange("127.0.0.1:8443#")).rejects.toThrow(OAuthUrlRefused);

        expect(sent.length).toBe(0);
    });
});
