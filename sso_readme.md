# Overview

SSO on Supabase is mainly managed using `supabase sso` cli commands. There are details in [Supabase Documentation](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml).

# Runbook

We have a run book to keep steps and processes documented on [Google Drive](https://docs.google.com/document/d/1UjynajGgmn0BXF61ooRHUZhmur66F5gSQWhBMMOtz-o/edit?usp=sharing)

# Attribute Mappings

These are stored in `/supabase/attrMaps` as JSON files for each provider. These are stored here to make running the `supabase sso update` command a bit easier. To know more about these please see [Supabase Documentation](https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml#understanding-attribute-mappings).
