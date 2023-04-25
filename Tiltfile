# This file is interpreted by `tilt`, and describes how to get a local flow environment running.
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
os.putenv("DATABASE_URL", DATABASE_URL)
os.putenv("RUST_LOG", "info")


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


local_resource('ops-catalog',
    cmd='./local/ops-publication.sh | psql "%s"' % DATABASE_URL,
    auto_init=False,
    trigger_mode=TRIGGER_MODE_MANUAL,
    resource_deps=['supabase'])

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
    --broker.address http://localhost:8080 \
    --broker.cache.size 128 \
    --consumer.limit 1024 \
    --consumer.max-hot-standbys 0 \
    --consumer.port 9000 \
    --consumer.host localhost \
    --etcd.address http://localhost:2379 \
    --flow.builds-root file://%s/ \
    --log.format text \
    --log.level info \
    --flow.network supabase_network_flow' % (REPO_BASE, FLOW_BUILDS_DIR),
    links='http://localhost:9000/debug/pprof',
    resource_deps=['etcd'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=9000, path='/debug/ready')
    ))

local_resource('agent', serve_cmd='%s/flow/.build/package/bin/agent \
    --connector-network supabase_network_flow \
    --bin-dir %s/flow/.build/package/bin' % (REPO_BASE, REPO_BASE),
    deps=[],
    resource_deps=['reactor', 'gazette'])

local_resource('config-encryption', serve_cmd='%s/config-encryption/target/debug/flow-config-encryption \
    --gcp-kms %s' % (REPO_BASE, TEST_KMS_KEY),
    deps=[])

local_resource(
    'edge-functions',
    serve_cmd='cd %s/flow && supabase functions serve oauth --env-file supabase/env.local --debug' % REPO_BASE,
    deps=['config-encryption'])

local_resource('ui', serve_dir='%s/ui' % REPO_BASE, serve_cmd='BROWSER=none npm start', deps=[], links='http://localhost:3000')

DPG_REPO='%s/data-plane-gateway' % REPO_BASE
DPG_TLS_CERT_PATH='%s/local-tls-cert.pem' % DPG_REPO
DPG_TLS_KEY_PATH='%s/local-tls-private-key.pem' % DPG_REPO

local_resource('dpg-tls-cert',
    dir='%s/data-plane-gateway' % REPO_BASE,
    cmd='[ -f %s ] || openssl req -x509 -nodes -days 365 \
        -subj  "/C=CA/ST=QC/O=Estuary/CN=localhost:28318" \
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

