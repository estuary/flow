# This file is interpreted by `tilt`, and describes how to get a local flow environment running.
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
os.putenv("DATABASE_URL", DATABASE_URL)
os.putenv("RUST_LOG", "info")
os.putenv("DOCKER_DEFAULT_PLATFORM", "linux/amd64")

# Secret used to sign Authorizations within a local data plane, as base64("supersecret").
# Also allow requests without an Authorization (to not break data-plane-gateway just yet).
AUTH_KEYS="c3VwZXJzZWNyZXQ=,AA=="
os.putenv("CONSUMER_AUTH_KEYS", AUTH_KEYS)
os.putenv("BROKER_AUTH_KEYS", AUTH_KEYS)


REPO_BASE= '%s/..' % os.getcwd()
TEST_KMS_KEY="projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/testing"

HOME_DIR=os.getenv("HOME")
FLOW_DIR=os.getenv("FLOW_DIR", os.path.join(HOME_DIR, "flow-local"))
ETCD_DATA_DIR=os.path.join(FLOW_DIR, "etcd")

FLOW_BUILDS_ROOT="file://"+os.path.join(FLOW_DIR, "builds")+"/"
# Or alternatively, use an actual bucket when testing with external data-planes:
# FLOW_BUILDS_ROOT="gs://example/builds/"

# A token for the local-stack system user signed against the local-stack
# supabase secret (super-secret-jwt-token-with-at-least-32-characters-long).
SYSTEM_USER_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjU0MzEvYXV0aC92MSIsInN1YiI6ImZmZmZmZmZmLWZmZmYtZmZmZi1mZmZmLWZmZmZmZmZmZmZmZiIsImF1ZCI6ImF1dGhlbnRpY2F0ZWQiLCJleHAiOjI3MDAwMDAwMDAsImlhdCI6MTcwMDAwMDAwMCwiZW1haWwiOiJzdXBwb3J0QGVzdHVhcnkuZGV2Iiwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJpc19hbm9ueW1vdXMiOmZhbHNlfQ.Nb-N4s_YnObBHGivSTe_8FEniVUUpehzrRkF5JgNWWU"

# Start supabase, which is needed in order to compile the agent
local_resource('supabase', cmd='supabase start', links='http://localhost:5433')

# Builds many of the binaries that we'll need
local_resource('make', cmd='make', resource_deps=['supabase'])

local_resource('etcd', serve_cmd='%s/flow/.build/package/bin/etcd \
    --data-dir %s \
    --log-level info \
    --logger zap' % (REPO_BASE, ETCD_DATA_DIR),
    resource_deps=['make'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=2379, path='/health')
    )
)

local_resource('gazette', serve_cmd='%s/flow/.build/package/bin/gazette serve \
    --broker.port=8080 \
    --broker.host=localhost \
    --broker.disable-stores \
    --broker.max-replication=1 \
    --log.level=info' % REPO_BASE,
    links='http://localhost:8080/debug/pprof',
    resource_deps=['etcd'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=8080, path='/debug/ready')
    )
)

local_resource('reactor', serve_cmd='%s/flow/.build/package/bin/flowctl-go serve consumer \
    --flow.allow-local \
    --broker.address http://localhost:8080 \
    --broker.cache.size 128 \
    --consumer.host localhost \
    --consumer.limit 1024 \
    --consumer.max-hot-standbys 0 \
    --consumer.port 9000 \
    --etcd.address http://localhost:2379 \
    --flow.builds-root %s \
    --flow.network supabase_network_flow \
    --flow.control-api http://localhost:8675 \
    --flow.data-plane-fqdn local-cluster.dp.estuary-data.com \
    --log.format text \
    --log.level info' % (REPO_BASE, FLOW_BUILDS_ROOT),
    links='http://localhost:9000/debug/pprof',
    resource_deps=['etcd'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=9000, path='/debug/ready')
    )
)

local_resource('agent', serve_cmd='%s/flow/.build/package/bin/agent \
    --connector-network supabase_network_flow \
    --allow-local \
    --allow-origin http://localhost:3000 \
    --api-port 8675 \
    --builds-root %s \
    --serve-handlers \
    --bin-dir %s/flow/.build/package/bin' % (REPO_BASE, FLOW_BUILDS_ROOT, REPO_BASE),
    resource_deps=['reactor', 'gazette']
)

local_resource('create-data-plane-local-cluster',
    cmd='sleep 5 && curl -v \
        -X POST \
        -H "content-type: application/json" \
        -H "authorization: bearer %s" \
        --data-binary \'{ \
            "name":"local-cluster",\
            "category": {\
                "manual": {\
                    "brokerAddress": "http://localhost:8080",\
                    "reactorAddress": "http://localhost:9000",\
                    "hmacKeys": ["c3VwZXJzZWNyZXQ="]\
                }\
            }\
        }\' http://localhost:8675/admin/create-data-plane' % SYSTEM_USER_TOKEN,
    resource_deps=['agent']
)

local_resource('update-l2-reporting',
    cmd='curl -v \
        -X POST \
        -H "content-type: application/json" \
        -H "authorization: bearer %s" \
        --data-binary \'{ \
            "defaultDataPlane":"ops/dp/public/local-cluster",\
            "dryRun":false\
        }\' http://localhost:8675/admin/update-l2-reporting' % SYSTEM_USER_TOKEN,
    resource_deps=['create-data-plane-local-cluster']
)

local_resource('local-ops-view',
    cmd='./local/ops-publication.sh ops-catalog/local-view.bundle.json | psql "%s"' % DATABASE_URL,
    resource_deps=['update-l2-reporting']
)

local_resource('config-encryption',
    serve_cmd='%s/config-encryption/target/debug/flow-config-encryption --gcp-kms %s' % (REPO_BASE, TEST_KMS_KEY)
)

local_resource(
    'edge-functions',
    serve_cmd='cd %s/flow && supabase functions serve --env-file supabase/env.local --import-map supabase/functions/import-map.json' % REPO_BASE,
    resource_deps=['config-encryption']
)

local_resource(
    'ui',
    serve_dir='%s/ui' % REPO_BASE,
    serve_cmd='BROWSER=none npm start',
    links='http://localhost:3000'
)

DPG_REPO='%s/data-plane-gateway' % REPO_BASE
DPG_TLS_CERT_PATH='%s/local-tls-cert.pem' % DPG_REPO
DPG_TLS_KEY_PATH='%s/local-tls-private-key.pem' % DPG_REPO

local_resource('dpg-tls-cert',
    dir='%s/data-plane-gateway' % REPO_BASE,
    # These incantations create a non-CA self-signed certificate which is
    # valid for localhost and its subdomains. rustls is quite fiddly about
    # accepting self-signed certificates so all of these are required.
    cmd='[ -f %s ] || openssl req -x509 -nodes -days 365 \
        -subj  "/ST=QC/O=Estuary/CN=localhost" \
        -addext basicConstraints=critical,CA:FALSE,pathlen:1 \
        -addext "subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1" \
        -newkey rsa:2048 -keyout "%s" \
        -out "%s"' % (DPG_TLS_KEY_PATH, DPG_TLS_KEY_PATH, DPG_TLS_CERT_PATH)
)

local_resource('data-plane-gateway',
    dir=DPG_REPO,
    serve_dir=DPG_REPO,
    cmd='go build .',
    serve_cmd='./data-plane-gateway \
        --tls-private-key=%s \
        --tls-certificate=%s \
        --broker-address=localhost:8080 \
        --consumer-address=localhost:9000 \
        --log.level=debug \
        --inference-address=localhost:9090 \
        --control-plane-auth-url=http://localhost:3000' % (
            DPG_TLS_KEY_PATH,
            DPG_TLS_CERT_PATH
        ),
    links='https://localhost:28318/',
    resource_deps=['gazette', 'reactor', 'dpg-tls-cert']
)

