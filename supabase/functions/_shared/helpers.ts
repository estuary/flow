import Mustache from 'npm:mustache';
import { fromUint8Array } from "https://deno.land/x/base64/base64url.ts"

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

// https://github.com/chiefbiiko/sha256/issues/5#issuecomment-1766746363
export async function hashStrBase64(str:string):Promise<string> {
  const msgUint8 = new TextEncoder().encode(str)
  const hashBuffer = await crypto.subtle.digest('SHA-256', msgUint8)
  return fromUint8Array(new Uint8Array(hashBuffer)).padEnd(44, '=')
}

// map a standard base64 encoding to a url-safe encoding
// see https://www.oauth.com/oauth2-servers/pkce/authorization-request/
export const base64URLSafe = (str: string) =>
    str.replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/\=+/, "");

export const generateUniqueRandomKey = (size: number = 40) => {
    const validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    let array = new Uint8Array(size) as any;
    crypto.getRandomValues(array);
    array = array.map((x: number) => validChars.codePointAt(x % validChars.length));
    
    return String.fromCharCode.apply(null, array);
};