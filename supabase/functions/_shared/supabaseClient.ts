import {createClient} from "npm:@supabase/supabase-js@^2.0.0";

export const supabaseClient = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);

// A client that acts as the caller. Lives for one request.
export const createSupabaseClientWithAuthorization = (authHeader: any) => {
    return createClient(
        Deno.env.get("SUPABASE_URL") ?? "",
        Deno.env.get("SUPABASE_ANON_KEY") ?? "",
        {
            global: {
                headers: { Authorization: authHeader },
            },
            // The defaults are for browsers. Outside a browser, autoRefreshToken
            // starts an interval that is never cleared. The other two do nothing
            // here. Turning them off makes it clear this is a server-side client.
            auth: {
                autoRefreshToken: false,
                persistSession: false,
                detectSessionInUrl: false,
            },
        },
    );
}