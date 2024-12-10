# Overview

SSO on Supabase is mainly managed using `supabase sso` cli commands. There are details in [Supabase Documentation](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml).

# Runbook

We have a run book to keep steps and processes documented on [Google Drive](https://docs.google.com/document/d/1UjynajGgmn0BXF61ooRHUZhmur66F5gSQWhBMMOtz-o/edit?usp=sharing)

# Attribute Mappings

These are stored in 1password under `SSO Attribute Mappings`. They are stored on a per-customer basis. This is because each implementation of SAML could be different between companies - even with the same provider.

To use an existing mapping you will want to copy the values into a *temporary* `json` file that is used to run `supabase sso update`. After you are done making changes please remember to update value in 1password. To know more about these please see [Supabase Documentation](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml#understanding-attribute-mappings).
