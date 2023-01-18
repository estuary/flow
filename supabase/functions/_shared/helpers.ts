import Mustache from 'https://esm.sh/mustache';
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
};

export const compileTemplate = (template: string, data: any, connector_id: string) => {
    const mustacheOutput = Mustache.render(template, {
        ...data,
        ...mustacheHelpers,
    });

    return mustacheOutput;
};
