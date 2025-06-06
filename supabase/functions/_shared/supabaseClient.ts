import {createClient} from "npm:@supabase/supabase-js@^2.0.0";

export const supabaseClient = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);

export const createSupabaseClientWithAuthorization = (authHeader: any) => {
    return createClient(
        Deno.env.get("SUPABASE_URL") ?? "",
        Deno.env.get("SUPABASE_ANON_KEY") ?? "",
        {
            global: {
                headers: { Authorization: authHeader },
            },
        },
    );
}