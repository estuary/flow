import { describe, it } from "jsr:@std/testing/bdd";
import { expect } from "jsr:@std/expect";

import { OAuthUrlRefused, renderValidatedTemplateUrl } from "../_shared/helpers.ts";

// Templates are grouped by the case the validator puts them in.
// Each one is the shape of a template in use, with invented hosts.
const TEMPLATES = {
    // Case A. The template names the host outright.
    fixedHost: "https://oauth.example.com/token",
    fixedHostWithQuery:
        "https://auth.example.com/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&state={{#urlencode}}{{{ state }}}{{/urlencode}}",

    // Case B. The caller names a subdomain of a host the template fixes.
    callerSubdomain: "https://{{{ config.subdomain }}}.example.com/oauth/tokens",

    // Case C. The caller names the whole host.
    callerHost: "https://{{{ config.address }}}/oauth/token",

    // Case A or C depending on input: an optional caller host, falling back to
    // one of two the template fixes. Salesforce's shape, and the reason
    // withSentinels has to preserve empty strings -- a sentinel in a blank
    // my_domain flips these sections and picks the wrong branch.
    optionalCallerHost:
        "https://{{#config.my_domain}}{{{config.my_domain}}}{{/config.my_domain}}{{^config.my_domain}}{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}.salesforce.com{{/config.my_domain}}/services/oauth2/token",
};

// Mirrors the render context built by oauth/access-token.ts.
const render = (template: string, provider: string, config: any) =>
    renderValidatedTemplateUrl(template, provider, (fromCaller) => ({
        redirect_uri: fromCaller("https://dashboard.estuary.dev/oauth"),
        client_id: "platform-client-id",
        client_secret: "platform-client-secret",
        config: fromCaller(config),
        code: fromCaller("an-authorization-code"),
        state: "an-opaque-state",
    }));

const hostOf = (template: string, provider: string, config: any) =>
    new URL(render(template, provider, config)).host;

const expectRefused = (template: string, provider: string, config: any) =>
    expect(() => render(template, provider, config)).toThrow(OAuthUrlRefused);

describe("renderValidatedTemplateUrl", () => {
    describe("case A: the template fixes the host", () => {
        const { fixedHost, fixedHostWithQuery } = TEMPLATES;

        it("renders unchanged whatever the caller sends", () => {
            expect(hostOf(fixedHost, "whoever", {})).toBe("oauth.example.com");
            expect(hostOf(fixedHostWithQuery, "whoever", {})).toBe("auth.example.com");
            expect(hostOf(fixedHost, "whoever", { anything: "attacker.example#" }))
                .toBe("oauth.example.com");
        });

        it("allows a fallback host chosen by the template", () => {
            // The caller supplies nothing, so these resolve to case A and are
            // validated by exact match. They fail if an empty string is ever
            // replaced by a sentinel.
            const sf = TEMPLATES.optionalCallerHost;
            expect(hostOf(sf, "salesforce", { my_domain: "", is_sandbox: false })).toBe("login.salesforce.com");
            expect(hostOf(sf, "salesforce", { my_domain: "", is_sandbox: true })).toBe("test.salesforce.com");
            expect(hostOf(sf, "salesforce", {})).toBe("login.salesforce.com");
        });
    });

    describe("case B: the caller supplies part of the host", () => {
        const template = TEMPLATES.callerSubdomain;

        it("allows a subdomain of the fixed domain", () => {
            expect(hostOf(template, "whoever", { subdomain: "acmeco" })).toBe("acmeco.example.com");
        });

        it("refuses a subdomain that escapes the host", () => {
            // Each of these ends the host early, so the request goes elsewhere.
            expectRefused(template, "whoever", { subdomain: "attacker.example#" });
            expectRefused(template, "whoever", { subdomain: "127.0.0.1:8443#" });
            expectRefused(template, "whoever", { subdomain: "attacker.example/x#" });
            expectRefused(template, "whoever", { subdomain: "acmeco@attacker.example#" });
        });

        it("refuses a lookalike domain", () => {
            expectRefused(template, "whoever", { subdomain: "acmeco.example.com.attacker.example#" });
        });

        it("refuses an empty subdomain, which is not a valid host", () => {
            expectRefused(template, "whoever", { subdomain: "" });
        });
    });

    describe("case C: the caller supplies the whole host", () => {
        const { callerHost, optionalCallerHost } = TEMPLATES;

        it("allows a host matching one of the provider's suffixes", () => {
            expect(hostOf(optionalCallerHost, "salesforce", { my_domain: "acme.my.salesforce.com" }))
                .toBe("acme.my.salesforce.com");
            expect(hostOf(optionalCallerHost, "salesforce", { my_domain: "acme--uat.sandbox.my.salesforce.com" }))
                .toBe("acme--uat.sandbox.my.salesforce.com");
        });

        it("refuses a host outside those suffixes", () => {
            expectRefused(optionalCallerHost, "salesforce", { my_domain: "attacker.example" });
            expectRefused(optionalCallerHost, "salesforce", {
                my_domain: "evil-my.salesforce.com.attacker.example",
            });
        });

        it("refuses every host when the provider has no entry", () => {
            expectRefused(callerHost, "unlisted", { address: "anything.example" });
            expectRefused(callerHost, "unlisted", { address: "attacker.example" });
        });
    });

    describe("checks applied whatever the host", () => {
        it("refuses a port the template did not specify", () => {
            expectRefused(TEMPLATES.callerSubdomain, "whoever", { subdomain: "acmeco:8443" });
            expectRefused(TEMPLATES.optionalCallerHost, "salesforce", {
                my_domain: "acme.my.salesforce.com:8443",
            });
        });

        it("refuses credentials smuggled into a host slot", () => {
            // The host itself stays in bounds, so only the credentials check
            // catches these. fetch would forward them as an Authorization header.
            expectRefused(TEMPLATES.callerSubdomain, "whoever", { subdomain: "user:pw@acmeco" });
            expectRefused(TEMPLATES.callerSubdomain, "whoever", { subdomain: "a@b.example.com" });
            expectRefused(TEMPLATES.optionalCallerHost, "salesforce", {
                my_domain: "user@acme.my.salesforce.com",
            });
        });

        it("refuses numbers and booleans in a host slot", () => {
            // 2852039166 is 169.254.169.254 as one integer, and the URL parser
            // reads an all-numeric host as IPv4. When only strings were
            // sentineled, both renders agreed and case A let it through.
            expectRefused(TEMPLATES.optionalCallerHost, "salesforce", { my_domain: 2852039166 });
            expectRefused(TEMPLATES.optionalCallerHost, "salesforce", { my_domain: 2130706433 });
            expectRefused(TEMPLATES.optionalCallerHost, "salesforce", { my_domain: true });
            expectRefused(TEMPLATES.callerHost, "salesforce", { address: 0 });
        });
    });

    describe("template shapes the validator does not support", () => {
        it("refuses a caller value inside a host label, even for benign input", () => {
            for (const template of [
                "https://api-{{{ config.region }}}.example.com/t",
                "https://{{{ config.region }}}-api.example.com/t",
            ]) {
                expect(() => render(template, "whoever", { region: "eu" }))
                    .toThrow("inside a host label");
            }
        });

        it("refuses more than one caller value in the host, even for benign input", () => {
            const template = "https://{{{ config.a }}}.{{{ config.b }}}/oauth";
            expect(() => render(template, "whoever", { a: "p", b: "q" }))
                .toThrow("more than one caller value");
            // With a fixed sentinel, supplying it as a value forged a marker and
            // landed this in case B. The sentinel is now drawn per render.
            expectRefused(template, "whoever", { a: "attacker.example", b: "xsentinelx" });
        });
    });
});
