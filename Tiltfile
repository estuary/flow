# This file is interpreted by `tilt`, and describes how to get a local flow environment running.
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
os.putenv("DATABASE_URL", DATABASE_URL)
os.putenv("RUST_LOG", "info")
os.putenv("DOCKER_DEFAULT_PLATFORM", "linux/amd64")


# Secret(s) used to sign Authorizations within a data plane.
# Testing values here are the base64 encoding of "secret" and "other-secret".
AUTH_KEYS="c2VjcmV0,b3RoZXItc2VjcmV0"
os.putenv("CONSUMER_AUTH_KEYS", AUTH_KEYS)
os.putenv("BROKER_AUTH_KEYS", AUTH_KEYS)


REPO_BASE= '%s/..' % os.getcwd()
TEST_KMS_KEY="projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/testing"

HOME_DIR=os.getenv("HOME")
FLOW_DIR=os.getenv("FLOW_DIR", os.path.join(HOME_DIR, "flow-local"))
ETCD_DATA_DIR=os.path.join(FLOW_DIR, "etcd")
FLOW_BUILDS_DIR=os.path.join(FLOW_DIR, "builds")

# Start supabase, which is needed in order to compile the agent
local_resource('supabase', cmd='supabase start', links='http://localhost:5433')

# Builds many of the binaries that we'll need
local_resource('make', cmd='make', resource_deps=['supabase'])

# The basic ops collections for logs and stats must be published before any other publication can
# succeed. This does not include any of the ops-related tasks, which themselves will require these
# collections to be present.
local_resource('ops-collections',
    cmd='./local/ops-publication.sh "base-collections.flow.yaml" | psql "%s"' % DATABASE_URL,
    resource_deps=['agent'])

local_resource('ops-catalog',
    cmd='./local/ops-publication.sh "template-local.flow.yaml" | psql "%s"' % DATABASE_URL,
    auto_init=False,
    trigger_mode=TRIGGER_MODE_MANUAL,
    resource_deps=['agent'])

local_resource('etcd', serve_cmd='%s/flow/.build/package/bin/etcd \
    --data-dir %s \
    --log-level info \
    --logger zap' % (REPO_BASE, ETCD_DATA_DIR),
    resource_deps=['make'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=2379, path='/health')
    ))

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
    ))

local_resource('reactor', serve_cmd='%s/flow/.build/package/bin/flowctl-go serve consumer \
    --flow.allow-local \
    --broker.address http://localhost:8080 \
    --broker.cache.size 128 \
    --consumer.host localhost \
    --consumer.limit 1024 \
    --consumer.max-hot-standbys 0 \
    --consumer.port 9000 \
    --etcd.address http://localhost:2379 \
    --flow.builds-root file://%s/ \
    --flow.enable-schema-inference \
    --flow.network supabase_network_flow \
    --log.format text \
    --log.level info' % (REPO_BASE, FLOW_BUILDS_DIR),
    links='http://localhost:9000/debug/pprof',
    resource_deps=['etcd'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=9000, path='/debug/ready')
    ))

local_resource('agent', serve_cmd='%s/flow/.build/package/bin/agent \
    --connector-network supabase_network_flow \
    --allow-local \
    --broker-address http://localhost:8080 \
    --consumer-address=http://localhost:9000 \
    --bin-dir %s/flow/.build/package/bin' % (REPO_BASE, REPO_BASE),
    deps=[],
    resource_deps=['reactor', 'gazette'])

local_resource('config-encryption', serve_cmd='%s/config-encryption/target/debug/flow-config-encryption \
    --gcp-kms %s' % (REPO_BASE, TEST_KMS_KEY),
    deps=[])

local_resource(
    'edge-functions',
    serve_cmd='cd %s/flow && supabase functions serve --env-file supabase/env.local --import-map supabase/functions/import-map.json' % REPO_BASE,
    deps=['config-encryption'])

local_resource('ui', serve_dir='%s/ui' % REPO_BASE, serve_cmd='BROWSER=none npm start', deps=[], links='http://localhost:3000')

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
        -out "%s"' % (DPG_TLS_KEY_PATH, DPG_TLS_KEY_PATH, DPG_TLS_CERT_PATH))

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
    resource_deps=['gazette', 'reactor', 'dpg-tls-cert'])

