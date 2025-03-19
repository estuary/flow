
# Log into Estuary Using SSO

You can register or log into the [Estuary dashboard](https://dashboard.estuary.dev/) with one of several methods. Sign in with either:
* A Google account
* A GitHub account
* An Azure account
* SSO (Single Sign-On)

While a Google, GitHub, or Azure account won't require additional configuration, there is a short setup process if you'd like to use SSO through another provider.

## Initial SSO Setup

To enable the SSO option on Estuary's end, [contact support](mailto:support@estuary.dev) and provide the following information:

1. **Metadata URL or Metadata XML File**

   Using the metadata URL is the preferred method of configuring SSO. However, if your provider doesn't support metadata URLs, you may send a metadata XML file instead.

2. **Company Domain**

   Provide the domain(s) used for your company's email addresses.

You will also need to configure SSO with your identity provider on your end. See [the list of details](#estuary-sso-details) below for information on Estuary that you can share with your identity provider.

After SSO is fully enabled for your domain, you may log into Estuary via the [SSO login page](https://dashboard.estuary.dev/sso/login) and create your tenant. This login flow is also accessible via the main sign in page.

## Add SSO Users to Your Tenant

You may invite additional members of your company to collaborate with you. Estuary supports Just-In-Time account creation rather than SCIM: a user account is generated on first login. Therefore, new users and the tenant admin will need to follow the steps below.

**New user**

1. A new user should enter their email to sign in via the [SSO login page](https://dashboard.estuary.dev/sso/login).

2. Estuary will redirect to your provider for the user to authenticate.

3. When the login flow redirects back to Estuary, the new user will need to read and accept Estuary's terms of service.

4. The new user should **NOT** create a new tenant.

**Tenant admin**

1. Meanwhile, the tenant admin can sign into their Estuary dashboard and navigate to the **Admin** tab.

2. Under [Account Access](../concepts/web-app.md#account-access), in the **Organization Membership** section, click the **Add Users** button.

3. Choose your desired prefix (such as your entire tenant or a defined subset), capability (admin or read access), and type (multi- or single-use). Then create an invite link.

   You can create an invite link for each user you wish to invite to your tenant or you can reuse a shared, multi-use link.

4. Securely share the link with the new user.

When the new user follows the invite link, they will be added to your tenant with the specified permissions.

## Estuary SSO Details

Estuary runs SSO authentication through Supabase. You can view more information about Supabase's SSO capabilities [here](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml).

This means that Estuary uses SAML 2.0 for authentication. Estuary's SAML 2.0 URLs are as follows:

| Name | Value |
| --- | --- |
| `EntityID` | `https://eyrcnmuzzyriypdajwdk.supabase.co/auth/v1/sso/saml/metadata` |
| Metadata URL | `https://eyrcnmuzzyriypdajwdk.supabase.co/auth/v1/sso/saml/metadata` |
| Metadata URL (download) | `https://eyrcnmuzzyriypdajwdk.supabase.co/auth/v1/sso/saml/metadata?download=true` |
| ACS URL | `https://eyrcnmuzzyriypdajwdk.supabase.co/auth/v1/sso/saml/acs` |
| SLO URL | `https://eyrcnmuzzyriypdajwdk.supabase.co/auth/v1/sso/slo` |
| `NameID` | Required `emailAddress` or `persistent` |

:::info Alternative Names
The Metadata URL is also known as the "Audience URL" or "SP Entity ID" while, in Okta, the ACS (Assertion Consumer Service) URL is also referred to as the "Single Sign-on URL."
:::

Note that sending the email address back is **required**. You may return it with the `email` key or, if preferred, one of the other [attribute names](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml#why-do-some-users-get-saml-assertion-does-not-contain-email-address) that Supabase supports.

Optionally, you may also return a `displayName` attribute with the user's first and last name as a single string.
