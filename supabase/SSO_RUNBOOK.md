# SSO Runbook

## Adding SSO to Estuary Flow

### Details customers need

Make sure that they understand we will be using SAML for this integration. This means we will not be interacting with their OIDC endpoints.

### Details we need from customers

**Provider**
We do not 100% need this - but it will make debugging things easier. Most likely will be one of the following: Okta, Azure, Google Workspaces (GSuite), or self hosted.

**Metadata URL or Metadata XML**
This is the metadata url that the user's organization needs to provide. This will normally be a URL that is from a provider like Okta or Azure.

Google does NOT support Metadata URL and the user MUST provide us a Metadata XML file.

**Domain**
We need to know the domain for the organization. This needs to be a simple string like `estuary.dev` or `acmeco.com`. This should match the domain used for their work emails. This can be more than one in case their organization has multiple domains for emails.

This will be used during login by users entering their work email and the UI parses off the domain and uses that to login with SSO.

**Known Constraints**
The provider **must** return an email address in the metadata. Details documented at the [Supabase SAML docs](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml#why-do-some-users-get-saml-assertion-does-not-contain-email-address).

**Details customers may need from us**
They should go to [https://docs.estuary.dev/getting-started/sso-setup/](https://docs.estuary.dev/getting-started/sso-setup/)

### How to get details

On the registration page and SSO login page we include a link to the `Contact Us` form on the marketing site. Adding SSO will be a manual process for a while. Since Supabase requires CLI commands it may stay un-automated until they change something.

When someone reaches out we'll need to gain the required details called out above.

Supabase has pretty detailed docs on adding a provider:
[https://supabase.com/docs/guides/platform/sso/okta](https://supabase.com/docs/guides/platform/sso/okta)

### How to register new customer with SSO

Before the new customer can sign in and create their tenant we will need to register their domain with an SSO provider. Then they can login and the sign up flow should work like someone using OAuth.

### How to add SSO (register domain with provider)

#### Add with Supabase CLI

Once we have the details someone from Estuary that has access to the Supabase CLI will need to update our project. This will be a manual process that is documented at the [Supabase SAML connection docs](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml#add-a-connection).

We could make a special edge function / estuary_support only admin page in the dashboard to allow this to be simpler… but that might end up being more work right away and can wait.

```bash
supabase login # log in first if needed
supabase sso add --type saml --project-ref eyrcnmuzzyriypdajwdk \
  --metadata-url 'https://example.okta.com/app/id/sso/saml/metadata' \
  --domains example.com
```

#### Run a test

> **TODO:** No clue how this will be done yet. This might include someone getting on a call with the customer/user and walking them through logging in with SSO and then seeing how things work.

Since Supabase returns the metadata xml I am thinking they do some testing themselves to make sure they can reach that server.

### How to configure Attribute Mapping (optional)

We might have times where we need to use attribute mappings to ensure the provider details given map properly to our authentication model. If this step is needed it will probably require a lot of testing and back and forth as we figure out what fields are being returned. Supabase mentions in their docs it is normally best to reach out to providers to figure these out.

If these are updated *while* a user is logged in their attributes will not change until they logout and back in.

We will probably want to keep the JSON files as templates within the `supabase/` directory so we can reference them in the future. This way they are easy to keep track of. This file should contain mappings for at least these:

- **Required:** `email`
- **Recommended:** `full_name`

Example attribute mapping:

```json
{
    "keys": {
        "email": { "name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress" },
        "full_name": { "name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name" }
    }
}
```

Details documented at the [Supabase attribute mapping docs](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml#understanding-attribute-mappings).

### How do I remove, update, list, etc.

The main documentation for Supabase SSO is at:
[https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml)

The API documentation:
[https://supabase.com/docs/reference/cli/supabase-sso](https://supabase.com/docs/reference/cli/supabase-sso)

### How to enable SSO on the Estuary dashboard

The UI controls showing/hiding SSO with the environment property `VITE_SHOW_SSO`.

### How to test a new provider locally

**TL;DR** - Run a minimalist Supabase instance in Docker. Add provider. Point local UI to it. See that it can authenticate (no real calls will work).

You need to make sure that your existing Supabase instance is fully shutdown before starting up the new one.

1. Clone this repository: [https://github.com/calvincchan/supabase-saml-demo](https://github.com/calvincchan/supabase-saml-demo)
2. Follow the instructions for setting up a new provider in that project
3. Update the local UI to use the new URL and key
4. Try logging into the app with SSO

At this point you will need to work out how authentication is working. If everything fails there is probably an issue with Supabase finding the `email` address and you need to setup a new attribute mapping. These are stored in `1password` and should look like this:

```json
{
    "keys": {
        "email": { "name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress" },
        "full_name": { "name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name" }
    }
}
```

### Troubleshooting login failure

The most common issue seen is that we aren't receiving an `email` field in the SAML response from the identity provider. We have also seen a lack of `Destination` in the response.

To troubleshoot, you can find the Supabase SSO attempt logs in [Supabase -> Logs -> Auth](https://supabase.com/dashboard/project/eyrcnmuzzyriypdajwdk/logs/auth-logs), filter for the customer domain in question, and see what errors come up.

#### Testing SSO Locally

This can get tricky and was only ever done before the new mise local stack approach.

Some important links that were used to get it finished:
- [https://calvincchan.com/blog/self-hosted-supabase-enable-sso](https://calvincchan.com/blog/self-hosted-supabase-enable-sso)
- [https://github.com/supabase/cli/issues/1335](https://github.com/supabase/cli/issues/1335)

---

## Email Template

> Hi \<customer\>,
>
> I've set this up for you so you can now sign in with SSO at the following URL: [https://dashboard.estuary.dev/sso/login](https://dashboard.estuary.dev/sso/login). Please try that and let me know how it goes.
>
> Our SSO details are available here: [https://docs.estuary.dev/getting-started/sso-setup/#estuary-sso-details](https://docs.estuary.dev/getting-started/sso-setup/#estuary-sso-details)
>
> After you've logged in via SSO (not email), you can use this one-time admin login URL to access your existing tenant: `https://dashboard.estuary.dev/login?grantToken=<GENERATE_TOKEN>`
>
> For any other users you'd like to (re)invite to your tenant, you can do so by creating them a link as well, documented [here](https://docs.estuary.dev/getting-started/sso-setup/#add-sso-users-to-your-tenant) under the "Tenant Admin" section.
>
> Note that our auth provider (Supabase) does not enforce uniqueness on email, so any accounts already created via email will be separate to accounts created via SSO, and new SSO accounts will need to be granted access to your tenant.
>
> Please reach out if you have any questions!
>
> Regards,
