---
description: Set up Estuary SSO login on your tenant for a specific company domain. Guide includes Estuary's SAML 2.0 URLs and adding new users to your tenant.
---

# Set Up SSO Login

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
   For example, see [Supabase's steps](https://supabase.com/docs/guides/platform/sso/azure#upload-saml-metadata) for working with metadata files and URLs for Azure.

2. **Company Domain**

   Provide the domain(s) used for your company's email addresses.

3. **Tenant Name** (if you have one)

   If you already created a tenant, include its name so support can associate your SSO provider with it. If you don't have a tenant yet, you can create one after signing in with SSO — see the next section.

You will also need to configure SSO with your identity provider on your end. See [the list of details](#estuary-sso-details) below for information on Estuary that you can share with your identity provider.

### Starting Fresh with SSO

You don't need an existing tenant — or a Google, GitHub, or Azure login — to set up SSO. To use SSO from the start:

1. [Contact support](mailto:support@estuary.dev) with your metadata and company domain, as described above.

2. Once support confirms SSO is enabled for your domain, sign in through the [SSO login page](https://dashboard.estuary.dev/sso/login). This login flow is also accessible via the main sign in page.

3. Create your tenant.

4. Reply to support with your tenant name so they can associate your SSO provider with the tenant. This lets invite links direct your teammates through SSO, and is required if you want to make SSO mandatory for your organization.

5. Invite your teammates, as described in [Add SSO Users to Your Tenant](#add-sso-users-to-your-tenant).

### Adding SSO to an Existing Tenant

If you created your tenant with a Google, GitHub, or Azure account, include the tenant name in your support request. After your SSO provider is associated with your tenant, sign in through the [SSO login page](https://dashboard.estuary.dev/sso/login) using the same email address: your permissions transfer automatically from your previous account to your new SSO identity.

### Requiring SSO

Estuary can also make SSO mandatory for your organization. When enabled, members whose email addresses use your company domain can no longer sign in with Google, GitHub, or Azure and are directed to SSO instead. Let support know if you'd like this enforced for your domain.

## Add SSO Users to Your Tenant

You may invite additional members of your company to collaborate with you. Estuary supports Just-In-Time account creation rather than SCIM: a user account is generated on first login. Therefore, new users and the tenant admin will need to follow the steps below.

**New user**

1. A new user should enter their email to sign in via the [SSO login page](https://dashboard.estuary.dev/sso/login).

2. Estuary will redirect to your provider for the user to authenticate.

3. When the login flow redirects back to Estuary, the new user will need to read and accept Estuary's terms of service.

4. The new user should **NOT** create a new tenant.

**Tenant admin**

1. Meanwhile, the tenant admin can sign into their Estuary dashboard and navigate to the **Admin** tab.

2. Under [Account Access](/guides/dashboard/admin/#account-access), in the **Organization Membership** section, click the **Add Users** button.

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
