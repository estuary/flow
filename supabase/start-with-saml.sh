#!/usr/bin/env bash
# Start local Supabase with SAML enabled via MockSAML.
#
# `supabase stop && supabase start` recreates the auth container from scratch,
# stripping any env vars injected by setup-mocksaml. This script runs that
# cycle and then re-injects the SAML env vars and ensures the MockSAML provider
# is registered — no need to rerun the full skill.
#
# Usage: ./supabase/start-with-saml.sh [--reset]
#   --reset  Run `supabase db reset` instead of stop/start (wipes DB data)

set -euo pipefail

# Generate a SAML signing key (PKCS#1).
# GoTrue needs this to sign outgoing SAMLRequests.
SAML_KEY=$(openssl genrsa -traditional 2048 2>/dev/null \
  | grep -v "^-----" \
  | tr -d '\n')

RESET=${1:-}
PSQL="psql postgresql://postgres:postgres@localhost:5432/postgres"

# Detect docker prefix (direct or via Lima VM)
if docker ps &>/dev/null; then
  DOCKER="docker"
elif limactl shell tiger docker ps &>/dev/null 2>&1; then
  DOCKER="limactl shell tiger docker"
else
  echo "Error: cannot reach Docker daemon. Start Docker or your Lima VM first." >&2
  exit 1
fi

# ── Step 1: Start Supabase ────────────────────────────────────────────────────

if [[ "$RESET" == "--reset" ]]; then
  echo "--- Running supabase db reset ---"
  supabase db reset
else
  echo "--- Running supabase stop/start ---"
  supabase stop
  # Remove the auth container if it still exists — supabase stop doesn't remove
  # containers that were manually recreated by this script (not tracked by
  # docker-compose), which causes a name conflict on the next start.
  $DOCKER rm -f supabase_auth_flow &>/dev/null || true
  supabase start
fi

# ── Step 2: Inject SAML env vars (skipped if already present) ────────────────

if $DOCKER exec supabase_auth_flow env 2>/dev/null | grep -q "GOTRUE_SAML_ENABLED=true"; then
  echo "SAML env vars already present — skipping container recreation."
else
  echo "--- Injecting SAML env vars into auth container ---"

  IMAGE=$($DOCKER inspect supabase_auth_flow --format '{{.Config.Image}}')
  NETWORK=$($DOCKER inspect supabase_auth_flow --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')

  $DOCKER inspect supabase_auth_flow --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep -v -E '^(PATH=|API_EXTERNAL_URL=)' \
    > /tmp/auth_env.txt
  echo "GOTRUE_SAML_ENABLED=true" >> /tmp/auth_env.txt
  echo "GOTRUE_SAML_PRIVATE_KEY=$SAML_KEY" >> /tmp/auth_env.txt
  echo "API_EXTERNAL_URL=http://127.0.0.1:5431/auth/v1" >> /tmp/auth_env.txt

  if [[ "$DOCKER" == limactl* ]]; then
    limactl copy /tmp/auth_env.txt tiger:/tmp/auth_env.txt
  fi

  $DOCKER stop supabase_auth_flow && $DOCKER rm supabase_auth_flow
  $DOCKER run -d \
    --name supabase_auth_flow \
    --network "$NETWORK" \
    --restart always \
    --env-file /tmp/auth_env.txt \
    "$IMAGE" auth

  sleep 3
  if $DOCKER logs supabase_auth_flow --tail 5 2>&1 | grep -q "GoTrue API started"; then
    echo "GoTrue is running with SAML enabled."
  else
    echo "Warning: GoTrue may not have started cleanly. Check logs:"
    $DOCKER logs supabase_auth_flow --tail 20
    exit 1
  fi
fi

# ── Step 3: Ensure MockSAML provider is registered ───────────────────────────

PROVIDER_ID=$($PSQL -t -c "SELECT id FROM auth.sso_providers LIMIT 1;" 2>/dev/null | tr -d ' \n')

if [[ -n "$PROVIDER_ID" ]]; then
  echo "MockSAML provider already registered (id: $PROVIDER_ID)."
else
  echo "--- Registering MockSAML provider ---"

  # Extract the sb_secret_... key from Kong config — Kong translates it to
  # the service_role JWT when used as an apikey header.
  SERVICE_ROLE_KEY=$($DOCKER exec supabase_kong_flow cat /home/kong/kong.yml \
    | grep -o "sb_secret_[A-Za-z0-9_-]*" | head -1 || true)

  if [[ -z "$SERVICE_ROLE_KEY" ]]; then
    echo "Error: could not determine service role key from Kong config." >&2
    exit 1
  fi

  read -r -p "Email domain to associate with MockSAML [example.com]: " DOMAIN
  DOMAIN="${DOMAIN:-example.com}"

  RESPONSE=$(curl -s -X POST 'http://127.0.0.1:5431/auth/v1/admin/sso/providers' \
    -H "apikey: $SERVICE_ROLE_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"type\":\"saml\",\"metadata_url\":\"https://mocksaml.com/api/saml/metadata\",\"domains\":[\"$DOMAIN\"]}")

  PROVIDER_ID=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null || true)

  if [[ -z "$PROVIDER_ID" ]]; then
    echo "Error: failed to register provider. Response: $RESPONSE" >&2
    exit 1
  fi

  echo "MockSAML provider registered (id: $PROVIDER_ID, domain: $DOMAIN)."
fi

# ── Step 4: Link provider to a tenant ────────────────────────────────────────

LINKED_TENANT=$($PSQL -t -c "SELECT tenant FROM public.tenants WHERE sso_provider_id = '$PROVIDER_ID' LIMIT 1;" 2>/dev/null | tr -d ' \n')

if [[ -n "$LINKED_TENANT" ]]; then
  echo "Provider already linked to tenant: $LINKED_TENANT"
else
  DEFAULT_TENANT=$($PSQL -t -c "SELECT tenant FROM public.tenants WHERE tenant NOT LIKE 'ops.%' ORDER BY tenant LIMIT 1;" 2>/dev/null | tr -d ' \n')

  echo ""
  echo "Available tenants:"
  $PSQL -t -c "SELECT tenant FROM public.tenants WHERE tenant NOT LIKE 'ops.%' ORDER BY tenant;" 2>/dev/null | tr -d ' ' | grep -v '^$'
  echo ""
  read -r -p "Tenant to link to MockSAML provider [$DEFAULT_TENANT]: " TENANT
  TENANT="${TENANT:-$DEFAULT_TENANT}"

  # Normalise: ensure trailing slash
  TENANT="${TENANT%/}/"

  $PSQL -c "UPDATE public.tenants SET sso_provider_id = '$PROVIDER_ID' WHERE tenant = '$TENANT';"
  echo "Linked provider to tenant: $TENANT"
fi

echo ""
echo "--- Setup complete ---"
echo "  Provider ID : $PROVIDER_ID"
echo "  SSO login   : curl -s -X POST 'http://127.0.0.1:5431/auth/v1/sso' -H 'Content-Type: application/json' -d '{\"provider_id\":\"$PROVIDER_ID\"}'"
