import { describe, it } from "jsr:@std/testing/bdd";
import { expect } from "jsr:@std/expect";

// supabaseClient.ts builds its client at module load, before the import below.
Deno.env.set("SUPABASE_URL", "http://localhost:9999");
Deno.env.set("SUPABASE_SERVICE_ROLE_KEY", "dummy-service-role-key");

const { handleRequest } = await import("../oauth/handler.ts");

const connectorConfig = {
    oauth2_client_id: "platform-client-id",
    oauth2_client_secret: "platform-client-secret",
    oauth2_injected_values: null,
    oauth2_spec: {
        provider: "zendesk",
        accessTokenUrlTemplate: "https://{{{ config.subdomain }}}.zendesk.com/oauth/tokens",
    },
};

const post = (body: unknown) =>
    handleRequest(new Request("http://localhost/", { method: "POST", body: JSON.stringify(body) }));

describe("handleRequest", () => {
    // Asserting the status rather than a thrown error is what makes this notice
    // if the awaits in handler.ts are ever removed as redundant.
    it("answers a refused OAuth URL with a 400", async () => {
        const res = await post({
            operation: "access-token",
            state: btoa(JSON.stringify({ connector_id: "00:00:00:00:00:00:00:01" })),
            code: "an-authorization-code",
            config: { subdomain: "attacker.example#" },
            connector_config: connectorConfig,
        });

        expect(res.status).toBe(400);
        expect((await res.json()).error).toContain("is not within .zendesk.com");
    });

    it("answers an unknown operation with a 400", async () => {
        const res = await post({ operation: "nope" });

        expect(res.status).toBe(400);
        expect((await res.json()).error).toBe("unknown_operation");
    });
});
