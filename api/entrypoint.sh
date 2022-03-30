#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Grab public JSON Web Key Sets signed by the auth service.
curl -s https://auth.estuary.dev/jwks -o /etc/trusted.jwks.json

# Start nginx. It will daemon itself into the background.
/usr/sbin/nginx

# Project the standard DATABASE_URL environment variable,
# which is presumptively an injected secret, into PostgREST's
# expected environment variable.
export PGRST_DB_URI=${DATABASE_URL}

# Run postgrest as nginx, as an available non-priveledged user.
# It will create a socket that's permissioned only to nginx,
# which works fine because nginx also drops to the nginx user.
exec runuser \
  --user nginx \
  --group nginx  \
  --preserve-environment \
  /bin/postgrest /etc/postgrest.conf