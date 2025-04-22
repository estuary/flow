import { createClient } from "https://esm.sh/@supabase/supabase-js@^2.0.0?target=deno";

export const supabaseClient = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);
