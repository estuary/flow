import HandlebarsJS from 'https://esm.sh/handlebars';
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

export const handlebarsHelpers = {
    urlencode: function (s: string) {
        return encodeURIComponent(s);
    },
    basicauth: function (user: string, password: string) {
        return btoa(`${user}:${password}`);
    },
};

export const compileTemplate = (template: string, data: any, connector_id: string) => {
    (HandlebarsJS as any).registerHelper(handlebarsHelpers);

    const compiledTemplate = (HandlebarsJS as any).compile(template);
    const handlebarsOutput = compiledTemplate(data);

    try {
        //Duplicate for testing Mustache
        const mustacheOutput = Mustache.render(template, {
            ...data,
            ...handlebarsHelpers,
        });

        if (handlebarsOutput !== mustacheOutput) {
            console.log(`oauth:templates:mismatch:${connector_id}`, { template });
        }
    } catch (e) {
        // This try catch is just to be super safe
        console.log(`oauth:templates:error:${connector_id}`, e);
    }

    return handlebarsOutput;
};
