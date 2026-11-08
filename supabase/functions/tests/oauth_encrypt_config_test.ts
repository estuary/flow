import { afterAll, describe, it } from "jsr:@std/testing/bdd";
import { expect } from "jsr:@std/expect";

// supabaseClient.ts builds its client at module load, before the import below.
Deno.env.set("SUPABASE_URL", "http://localhost:9999");
Deno.env.set("SUPABASE_SERVICE_ROLE_KEY", "dummy-service-role-key");

// Answers for each PostgREST table the function queries, keyed by table name.
let tables: Record<string, () => Response> = {};

const realFetch = globalThis.fetch;
globalThis.fetch = ((url: any) => {
    const table = new URL(String(url)).pathname.split("/").pop()!;
    return Promise.resolve(tables[table]());
}) as any;

const { encryptConfig } = await import("../oauth/encrypt-config.ts");

// How PostgREST answers a `.single()` query that matched no row.
const noRows = () =>
    new Response(
        JSON.stringify({
            code: "PGRST116",
            details: "The result contains 0 rows",
            hint: null,
            message: "JSON object requested, multiple (or no) rows returned",
        }),
        { status: 406, headers: { "content-type": "application/json" } },
    );

const oneRow = (row: unknown) => () =>
    new Response(JSON.stringify(row), { status: 200, headers: { "content-type": "application/json" } });

describe("encryptConfig", () => {
    afterAll(() => {
        globalThis.fetch = realFetch;
    });

    const encrypt = () =>
        encryptConfig({
            connector_id: "00:00:00:00:00:00:00:01",
            connector_tag_id: "00:00:00:00:00:00:00:02",
            config: {},
        });

    it("answers a missing connector with the lookup error", async () => {
        tables = { connectors: noRows };

        const res = await encrypt();

        expect(res.status).toBe(400);
        expect((await res.json()).error.code).toBe("PGRST116");
    });

    it("answers a missing connector tag with the lookup error", async () => {
        tables = {
            connectors: oneRow({ oauth2_client_id: "cid", oauth2_client_secret: "sec", oauth2_injected_values: null }),
            connector_tags: noRows,
        };

        const res = await encrypt();

        expect(res.status).toBe(400);
        expect((await res.json()).error.code).toBe("PGRST116");
    });
});
