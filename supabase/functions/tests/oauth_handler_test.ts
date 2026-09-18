import { afterAll, beforeEach, describe, it } from "jsr:@std/testing/bdd";
import { expect } from "jsr:@std/expect";

// supabaseClient.ts builds its client at module load, before the import below.
// The edge runtime injects all three of these in deployment.
Deno.env.set("SUPABASE_URL", "http://localhost:9999");
Deno.env.set("SUPABASE_SERVICE_ROLE_KEY", "dummy-service-role-key");
Deno.env.set("SUPABASE_ANON_KEY", "dummy-anon-key");

const { handleRequest } = await import("../oauth/handler.ts");

const USER_TOKEN = "a-real-user-access-token";
const ANON_TOKEN = "the-public-anon-key";

// Stands in for GoTrue's /auth/v1/user: a user token resolves to a user, and
// anything else is refused the way the anonymous key is, since it has no
// user behind it. Every other outbound request is a test failure.
let userLookups = 0;
const realFetch = globalThis.fetch;
globalThis.fetch = ((url: any, init: any) => {
    const path = new URL(String(url)).pathname;
    if (!path.endsWith("/auth/v1/user")) {
        throw new Error(`unexpected outbound request to ${path}`);
    }
    userLookups += 1;
    const token = new Headers(init?.headers).get("authorization");
    if (token === `Bearer ${USER_TOKEN}`) {
        return Promise.resolve(new Response(
            JSON.stringify({ id: "00000000-0000-0000-0000-000000000001", aud: "authenticated", role: "authenticated" }),
            { status: 200, headers: { "content-type": "application/json" } },
        ));
    }
    return Promise.resolve(new Response(
        JSON.stringify({ code: 401, msg: "invalid claim: missing sub claim" }),
        { status: 401, headers: { "content-type": "application/json" } },
    ));
}) as any;

const connectorConfig = {
    oauth2_client_id: "platform-client-id",
    oauth2_client_secret: "platform-client-secret",
    oauth2_injected_values: null,
    oauth2_spec: {
        provider: "zendesk",
        accessTokenUrlTemplate: "https://{{{ config.subdomain }}}.zendesk.com/oauth/tokens",
    },
};

const inlineSpecRequest = {
    operation: "access-token",
    state: btoa(JSON.stringify({ connector_id: "00:00:00:00:00:00:00:01" })),
    code: "an-authorization-code",
    config: { subdomain: "attacker.example#" },
    connector_config: connectorConfig,
};

const post = (body: unknown, token?: string) =>
    handleRequest(new Request("http://localhost/", {
        method: "POST",
        body: JSON.stringify(body),
        headers: token ? { Authorization: `Bearer ${token}` } : {},
    }));

describe("handleRequest", () => {
    afterAll(() => {
        globalThis.fetch = realFetch;
    });
    beforeEach(() => {
        userLookups = 0;
    });

    // Asserting the status rather than a thrown error is what makes this notice
    // if the awaits in handler.ts are ever removed as redundant.
    it("answers a refused OAuth URL with a 400", async () => {
        const res = await post(inlineSpecRequest, USER_TOKEN);

        expect(res.status).toBe(400);
        expect((await res.json()).error).toContain("is not within .zendesk.com");
    });

    it("answers an unknown operation with a 400", async () => {
        const res = await post({ operation: "nope" });

        expect(res.status).toBe(400);
        expect((await res.json()).error).toBe("unknown_operation");
    });

    describe("an inline connector_config", () => {
        it("is refused without an Authorization header", async () => {
            const res = await post(inlineSpecRequest);

            expect(res.status).toBe(401);
            expect((await res.json()).error).toBe("Missing Authorization header");
            expect(userLookups).toBe(0);
        });

        it("is refused for a token with no user behind it", async () => {
            const res = await post(inlineSpecRequest, ANON_TOKEN);

            expect(res.status).toBe(401);
            expect((await res.json()).error).toBe("User not found");
            expect(userLookups).toBe(1);
        });

        it("is accepted for an authenticated user", async () => {
            const res = await post(inlineSpecRequest, USER_TOKEN);

            // Past the gate, this request fails on its escaped host as before.
            expect(res.status).toBe(400);
            expect(userLookups).toBe(1);
        });

        it("is not consulted when the spec comes from the connectors table", async () => {
            await post({ operation: "nope" });

            expect(userLookups).toBe(0);
        });
    });
});
