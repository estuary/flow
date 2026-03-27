---
name: setup-mocksaml
description: Set up MockSAML as a local SAML IdP for testing SSO flows (grant migration, invite enforcement). Run when setting up a fresh local env or after `supabase db reset`.
disable-model-invocation: true
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

GoTrue requires PKCS#1 format. Check the header of the generated key:

```bash
head -1 /tmp/saml_key.pem
```

- `BEGIN RSA PRIVATE KEY` → PKCS#1, good to go.
- `BEGIN PRIVATE KEY` → PKCS#8 (OpenSSL 3.x default). Convert it:
  ```bash
  openssl rsa -traditional -in /tmp/saml_key.pem -out /tmp/saml_key.pem
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

Build an env file for the new container. Using `--env-file` avoids shell
parsing issues with values that contain template syntax (e.g.
`GOTRUE_SMS_TEMPLATE=Your code is {{ .Code }}`).

```bash
grep -v -E '^(PATH=|API_EXTERNAL_URL=)' /tmp/auth_env.txt > /tmp/auth_env_filtered.txt
echo "GOTRUE_SAML_ENABLED=true" >> /tmp/auth_env_filtered.txt
echo "GOTRUE_SAML_PRIVATE_KEY=$SAML_KEY_B64" >> /tmp/auth_env_filtered.txt
echo "API_EXTERNAL_URL=http://127.0.0.1:5431/auth/v1" >> /tmp/auth_env_filtered.txt
```

**Important:** the `API_EXTERNAL_URL` override includes the `/auth/v1` prefix —
GoTrue uses this to generate the SAML ACS callback URL, and without the prefix
Kong won't route the callback correctly.

If using Lima, copy the env file into the VM before running docker:

```bash
limactl copy /tmp/auth_env_filtered.txt <vm>:/tmp/auth_env_filtered.txt
```

Stop and remove the old container, then recreate:

```bash
<docker-prefix> docker stop supabase_auth_flow && <docker-prefix> docker rm supabase_auth_flow

<docker-prefix> docker run -d \
  --name supabase_auth_flow \
  --network <network-name> \
  --restart always \
  --env-file /tmp/auth_env_filtered.txt \
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

If `supabase status` fails (e.g. Docker runs inside a Lima VM), read the key
from Kong's config instead — it's always accessible since Kong handles routing:

```bash
<docker-prefix> docker exec supabase_kong_flow cat /home/kong/kong.yml
```

Look for the `service_role` JWT in the authorization header rewriting rules.

### 7. Check if MockSAML is already registered

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres \
  -c "SELECT id FROM auth.sso_providers LIMIT 1;"
```

If a provider already exists, confirm with the user whether to reuse it or
register a new one. If reusing, skip to step 9.

### 8. Register MockSAML as an SSO provider

Ask the user which email domain to associate with the SSO provider (default:
`example.com`). This controls which email addresses are routed through SAML
login. MockSAML's default test user is `jackson@example.com`, so `example.com`
works out of the box — but the user may want a different domain to match their
test data.

```bash
curl -X POST 'http://127.0.0.1:5431/auth/v1/admin/sso/providers' \
  -H 'Authorization: Bearer <SERVICE_ROLE_KEY>' \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "saml",
    "metadata_url": "https://mocksaml.com/api/saml/metadata",
    "domains": ["<DOMAIN>"]
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
- `supabase stop && supabase start` replaces the auth container (loses SAML env vars) — use `./supabase/start-with-saml.sh` instead, which re-injects them automatically
- `supabase db reset` wipes SSO provider registrations — use `./supabase/start-with-saml.sh --reset` then rerun from step 8
