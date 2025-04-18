import Mustache from 'https://esm.sh/mustache?target=deno';
import { corsHeaders } from './cors.ts';

export const returnPostgresError = (error: any) => {
    return new Response(JSON.stringify({ error }), {
        headers: {
            ...corsHeaders,
            'Content-Type': 'application/json',
        },
        status: 400,
    });
};

export const mustacheHelpers = {
    urlencode: function (s: any) {
        return (s: string, render: any) => {
          return encodeURIComponent(render(s));
        }
    },
    basicauth: function (s: any, b: any) {
      return (s: string, render: any) => {
        return btoa(render(s));
      }
    },
    now_plus: function(s: any) {
      return (s: string, render: any) => {
        const now = new Date();
        const inputSeconds = parseInt(render(s));
        const newDate = new Date(now.getTime() + inputSeconds * 1000);

        return newDate.toISOString()
      }
    }
};

export const compileTemplate = (template: string, data: any) => {
    const mustacheOutput = Mustache.render(template, {
        ...data,
        ...mustacheHelpers,
    });

    return mustacheOutput;
};
