import { serve } from "https://deno.land/std@0.184.0/http/server.ts";

import { handleRequest } from "./handler.ts";

serve(handleRequest);
