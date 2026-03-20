---
name: setup-mocksaml
description: Set up MockSAML as a local SAML IdP for testing SSO flows (grant migration, invite enforcement). Run when setting up a fresh local env or after `supabase db reset`.
disable-model-invocation: true
allowed-tools: Bash(*), Read
---

# Set Up MockSAML for Local SSO Testing

This skill configures the local Supabase environment to support SAML SSO login
via mocksaml.com, enabling end-to-end testing of SSO grant migration (phase 4a)
and invite link SSO enforcement (phase 4b).

## Prerequisites

- Local Supabase must be running (`supabase start` or `supabase db reset`)
- Docker must be accessible (may require `limactl shell <vm>` prefix if
  Supabase runs in a Lima VM — check the memory file for this project)

## Steps

Execute these steps in order. Stop and report if any step fails.

### 1. Detect Docker access

Try `docker ps` directly. If that fails with a Docker daemon error, try
`limactl shell tiger docker ps`. Use whichever works as the docker command
prefix for all subsequent steps. If neither works, stop and tell the user
to start Docker or their Lima VM.

### 2. Verify the auth container is running

```bash
<docker-prefix> docker ps --filter name=supabase_auth_flow --format '{{.Status}}'
```

If not running, tell the user to run `supabase start` first.

### 3. Check if SAML is already enabled

```bash
<docker-prefix> docker exec supabase_auth_flow env | grep GOTRUE_SAML_ENABLED
```

If `GOTRUE_SAML_ENABLED=true` is already set, skip to step 6.

### 4. Generate a SAML signing key

```bash
openssl genrsa 2048 > /tmp/saml_key.pem
```

Strip to raw base64 (no PEM headers, no newlines):

```bash
SAML_KEY_B64=$(grep -v "^-----" /tmp/saml_key.pem | tr -d '\n')
```

### 5. Recreate the auth container with SAML enabled

Capture the current container's env vars, image, and network:

```bash
<docker-prefix> docker inspect supabase_auth_flow --format '{{range .Config.Env}}{{println .}}{{end}}' > /tmp/auth_env.txt
<docker-prefix> docker inspect supabase_auth_flow --format '{{.Config.Image}}'
# Network is typically supabase_network_flow — confirm via:
<docker-prefix> docker inspect supabase_auth_flow --format '{{json .NetworkSettings.Networks}}'
```

Stop and remove the old container, then recreate with all original env vars
plus the SAML vars. **Important:** override `API_EXTERNAL_URL` to include the
`/auth/v1` prefix — GoTrue uses this to generate the SAML ACS callback URL,
and without the prefix Kong won't route the callback correctly.

```bash
<docker-prefix> docker stop supabase_auth_flow && <docker-prefix> docker rm supabase_auth_flow

<docker-prefix> docker run -d \
  --name supabase_auth_flow \
  --network <network-name> \
  --restart always \
  -e GOTRUE_SAML_ENABLED=true \
  -e GOTRUE_SAML_PRIVATE_KEY=$SAML_KEY_B64 \
  -e API_EXTERNAL_URL=http://127.0.0.1:5431/auth/v1 \
  <all original -e flags from /tmp/auth_env.txt, excluding PATH= and API_EXTERNAL_URL=> \
  <image> auth
```

Verify it started successfully (look for "GoTrue API started on"):

```bash
sleep 3 && <docker-prefix> docker logs supabase_auth_flow --tail 5
```

If it's crash-looping, check the logs for the error and report to the user.

### 6. Get the service role key

```bash
supabase status --output json
```

Extract `SERVICE_ROLE_KEY` from the output.

### 7. Check if MockSAML is already registered

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres \
  -c "SELECT id FROM auth.sso_providers LIMIT 1;"
```

If a provider already exists, confirm with the user whether to reuse it or
register a new one. If reusing, skip to step 9.

### 8. Register MockSAML as an SSO provider

```bash
curl -X POST 'http://127.0.0.1:5431/auth/v1/admin/sso/providers' \
  -H 'Authorization: Bearer <SERVICE_ROLE_KEY>' \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "saml",
    "metadata_url": "https://mocksaml.com/api/saml/metadata",
    "domains": ["example.com"]
  }'
```

Save the `id` from the response.

### 9. Link a test tenant to the SSO provider

Ask the user which tenant to configure. Then:

```sql
UPDATE tenants
SET sso_provider_id = '<PROVIDER_UUID>'
WHERE tenant = '<tenant>/';
```

Verify:

```sql
SELECT tenant, sso_provider_id FROM tenants WHERE sso_provider_id IS NOT NULL;
```

### 10. Test the SSO flow

Initiate an SSO login to verify everything works:

```bash
curl -s -X POST 'http://127.0.0.1:5431/auth/v1/sso' \
  -H 'Content-Type: application/json' \
  -d '{"provider_id": "<PROVIDER_UUID>"}' | python3 -m json.tool
```

If the response contains a redirect URL, the setup is working. Tell the user
they can open that URL in a browser to complete the MockSAML login
(default user: jackson@example.com).

## Teardown

No teardown needed. Be aware:
- `supabase stop && supabase start` replaces the auth container (loses SAML config) — rerun this skill
- `supabase db reset` wipes SSO provider registrations — rerun from step 8
