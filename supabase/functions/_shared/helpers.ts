import { corsHeaders } from "./cors.ts";

export const returnPostgresError = (error: any) => {
  return new Response(JSON.stringify({ error }), {
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json",
    },
    status: 400,
  });
};

export const handlebarsHelpers = {
  urlencode: function(s: string) {
    return encodeURIComponent(s)
  }
}
