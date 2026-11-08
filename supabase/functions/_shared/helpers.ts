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

// Host suffixes each provider may reach. A provider needs an entry only when
// its template leaves the whole host to the caller. Providers not listed here
// fail closed.
export const ALLOWED_HOST_SUFFIXES: Record<string, string[]> = {
    salesforce: [".my.salesforce.com"],
};

// Matches a well-formed DNS name. Empty labels and embedded delimiters are
// rejected. `URL.hostname` is already lowercased and punycoded, so ASCII
// lowercase is enough.
const HOSTNAME_RE =
    /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

// A fresh sentinel for each render, so a caller cannot supply it and forge a
// marker where the template has none. The leading letter keeps an all-digit
// draw from parsing as an IPv4 address.
const newSentinel = (): string =>
    "x" + Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");

// Replaces every scalar in a render context with the sentinel. Rendering a
// template with the result shows where caller values land in the URL.
//
// Numbers count: they render as digits, and the URL parser reads an all-numeric
// host as an IPv4 address. Truthiness is preserved so that sections select the
// same branch in both renders: a truthy scalar becomes the sentinel and a falsy
// one becomes "", which a template like Salesforce's relies on to pick its
// fallback host.
const withSentinels = (sentinel: string) => {
    const replace = (v: any): any => {
        if (Array.isArray(v)) return v.map(replace);
        if (v !== null && typeof v === "object") {
            return Object.fromEntries(
                Object.entries(v).map(([k, inner]) => [k, replace(inner)]),
            );
        }
        if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
            return v ? sentinel : "";
        }
        return v;
    };
    return replace;
};

// A rendered OAuth URL that failed validation. It must not be requested.
export class OAuthUrlRefused extends Error {}

// Renders an OAuth2 URL template and returns the URL. Throws OAuthUrlRefused if
// a caller value escaped its slot in the host. `provider` keys the allowlist.
//
// `buildContext` runs twice, with the same field ordering each time. The first
// render produces the real URL. The second replaces caller values with
// sentinels. Comparing the two hosts shows which labels the template fixes and
// which the caller supplies.
export const renderValidatedTemplateUrl = (
    template: string,
    provider: string,
    buildContext: (fromCaller: (v: any) => any) => any,
): string => {
    const sentinel = newSentinel();
    const url = compileTemplate(template, buildContext((v) => v));
    const skeleton = compileTemplate(template, buildContext(withSentinels(sentinel)));

    let actual: URL, reference: URL;
    try {
        actual = new URL(url);
        reference = new URL(skeleton);
    } catch {
        throw new OAuthUrlRefused("OAuth URL template did not render a valid URL");
    }

    if (actual.protocol !== reference.protocol) {
        throw new OAuthUrlRefused("OAuth URL scheme changed");
    }
    if (actual.port !== reference.port) {
        throw new OAuthUrlRefused("OAuth URL port changed");
    }
    // Anything before an `@` in a caller value is parsed as credentials, not
    // host, so no check below sees it, and fetch forwards it as an Authorization
    // header. No template currently puts credentials in a URL.
    if (actual.username !== "" || actual.password !== "") {
        throw new OAuthUrlRefused("OAuth URL carries credentials");
    }
    if (!HOSTNAME_RE.test(actual.hostname)) {
        throw new OAuthUrlRefused(`OAuth URL host ${actual.host} is malformed`);
    }

    const labels = reference.hostname.split(".");
    const marker = labels.indexOf(sentinel);

    // Case A: the template fixes the whole host, like accounts.google.com.
    // Nothing caller-supplied is in it, so it must come out byte-identical.
    if (marker === -1) {
        // A sentinel inside a label means the template mixes caller text into a
        // fixed label, as in `api-{{{config.region}}}`. Label comparison cannot
        // separate the two, so refuse outright rather than fall through to an
        // exact match that fails every real request with a misleading message.
        if (reference.hostname.includes(sentinel)) {
            throw new OAuthUrlRefused(
                "OAuth URL template places a caller value inside a host label, which is not supported",
            );
        }
        if (actual.host !== reference.host) {
            throw new OAuthUrlRefused(
                `OAuth URL host ${actual.host} does not match ${reference.host}`,
            );
        }
        return url;
    }

    // Everything after the marker is fixed template text and empty when the
    // caller supplied the whole host. A second caller value in it leaves nothing
    // fixed to anchor on.
    const suffix = labels.slice(marker + 1).join(".");
    if (suffix.includes(sentinel)) {
        throw new OAuthUrlRefused(
            "OAuth URL template places more than one caller value in the host, which is not supported",
        );
    }

    // Case B: the template fixes a trailing domain such as ".zendesk.com", and
    // the caller supplies the part in front of it. The rendered host must still
    // end in that domain.
    if (suffix.length > 0) {
        if (!actual.hostname.endsWith("." + suffix)) {
            throw new OAuthUrlRefused(
                `OAuth URL host ${actual.host} is not within .${suffix}`,
            );
        }
        return url;
    }

    // Case C: the caller supplied the entire host. The template fixes no part of
    // it to compare against, so the host must match one of this provider's
    // allowed suffixes. No entry means nothing matches.
    const allowed = ALLOWED_HOST_SUFFIXES[provider] ?? [];
    if (!allowed.some((s) => actual.hostname.endsWith(s))) {
        throw new OAuthUrlRefused(
            `OAuth URL host ${actual.host} is not an allowed host for ${provider}`,
        );
    }
    return url;
};
