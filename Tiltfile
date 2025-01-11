# This file is interpreted by `tilt`, and describes how to get a local flow environment running.
DATABASE_URL="postgresql://postgres:postgres@db.flow.localhost:5432/postgres"

# Secret used to sign Authorizations within a local data plane, as base64("supersecret").
# Also allow requests without an Authorization (to not break data-plane-gateway just yet).
AUTH_KEYS="c3VwZXJzZWNyZXQ=,AA=="

REPO_BASE= '%s/..' % os.getcwd()
TEST_KMS_KEY="projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/testing"

FLOW_DIR=os.getenv("FLOW_DIR", os.path.join(os.getenv("HOME"), "flow-local"))
ETCD_DATA_DIR=os.path.join(FLOW_DIR, "etcd")

FLOW_BUILDS_ROOT="file://"+os.path.join(FLOW_DIR, "builds")+"/"

# A token for the local-stack system user signed against the local-stack
# supabase secret (super-secret-jwt-token-with-at-least-32-characters-long).
SYSTEM_USER_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjU0MzEvYXV0aC92MSIsInN1YiI6ImZmZmZmZmZmLWZmZmYtZmZmZi1mZmZmLWZmZmZmZmZmZmZmZiIsImF1ZCI6ImF1dGhlbnRpY2F0ZWQiLCJleHAiOjI3MDAwMDAwMDAsImlhdCI6MTcwMDAwMDAwMCwiZW1haWwiOiJzdXBwb3J0QGVzdHVhcnkuZGV2Iiwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJpc19hbm9ueW1vdXMiOmZhbHNlfQ.Nb-N4s_YnObBHGivSTe_8FEniVUUpehzrRkF5JgNWWU"

# Paths for CA and server certificates
CA_KEY_PATH = "%s/ca.key" % FLOW_DIR
CA_CERT_PATH = "%s/ca.crt" % FLOW_DIR
TLS_KEY_PATH = "%s/server.key" % FLOW_DIR
TLS_CERT_PATH = "%s/server.crt" % FLOW_DIR

local_resource(
    'supabase',
    cmd='supabase start',
    links='http://db.flow.localhost:5433',
)

local_resource(
    'make',
    cmd='make',
    resource_deps=['supabase'],
)

local_resource(
    'self-signed-tls-cert',
    dir=REPO_BASE,
    cmd = '''
    if [ ! -f "%s" ] || [ ! -f "%s" ]; then
        mkdir -p $(dirname "%s")

        openssl req -x509 -nodes -days 3650 \
            -subj "/C=US/ST=QC/O=Estuary/CN=Estuary Root CA" \
            -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
            -keyout "%s" \
            -out "%s"

        openssl req -nodes -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
            -subj "/C=US/ST=QC/O=Estuary/CN=flow.localhost" \
            -addext "subjectAltName=DNS:flow.localhost,DNS:*.flow.localhost,IP:127.0.0.1" \
            -keyout "%s" -out server.csr

        echo "subjectAltName=DNS:flow.localhost,DNS:*.flow.localhost,IP:127.0.0.1" > extfile.txt
        echo "basicConstraints=CA:FALSE" >> extfile.txt
        openssl x509 -req -days 365 \
            -in server.csr -CA "%s" -CAkey "%s" -CAcreateserial \
            -out "%s" \
            -extfile extfile.txt

        rm server.csr extfile.txt
    fi
    ''' % (
        TLS_CERT_PATH,   # Check if server certificate exists
        TLS_KEY_PATH,    # Check if server key exists
        TLS_CERT_PATH,   # Server certificate path for mkdir
        CA_KEY_PATH,     # CA key output path (ECDSA)
        CA_CERT_PATH,    # CA certificate output path
        TLS_KEY_PATH,    # Server key output path (ECDSA)
        CA_CERT_PATH,    # CA certificate input path
        CA_KEY_PATH,     # CA key input path
        TLS_CERT_PATH    # Server certificate output path
    )
)

local_resource(
    'etcd',
    serve_cmd='%s/flow/.build/package/bin/etcd \
    --data-dir %s \
    --log-level info \
    --logger zap' % (REPO_BASE, ETCD_DATA_DIR),
    resource_deps=['make'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=2379, path='/health')
    )
)

[local_resource(
    'gazette-%d' % port,
    serve_cmd='%s/flow/.build/package/bin/gazette serve' % REPO_BASE,
    serve_env={
        "BROKER_ALLOW_ORIGIN": "http://localhost:3000",
        "BROKER_AUTH_KEYS": AUTH_KEYS,
        "BROKER_AUTO_SUSPEND": "true",
        "BROKER_FILE_ONLY": "true",
        "BROKER_FILE_ROOT": FLOW_DIR,
        "BROKER_HOST": "gazette.flow.localhost",
        "BROKER_PEER_CA_FILE": CA_CERT_PATH,
        "BROKER_PORT": "%d" % port,
        "BROKER_SERVER_CERT_FILE": TLS_CERT_PATH,
        "BROKER_SERVER_CERT_KEY_FILE": TLS_KEY_PATH,
        "ETCD_ADDRESS": "http://etcd.flow.localhost:2379",
        "LOG_LEVEL": "info",
    },
    links='https://gazette.flow.localhost:%d/debug/pprof' % port,
    resource_deps=['etcd'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=port, path='/debug/ready', scheme='https')
    )
) for port in range(8080, 8084)]

[local_resource(
    'reactor-%d' % port,
    serve_cmd='%s/flow/.build/package/bin/flowctl-go serve consumer' % (REPO_BASE),
    serve_env={
        "BROKER_ADDRESS": "https://gazette.flow.localhost:8080",
        "BROKER_AUTH_KEYS": AUTH_KEYS,
        "BROKER_CACHE_SIZE": "128",
        "BROKER_FILE_ROOT": FLOW_DIR,
        "BROKER_TRUSTED_CA_FILE": CA_CERT_PATH,
        "CONSUMER_ALLOW_ORIGIN": "http://localhost:3000",
        "CONSUMER_AUTH_KEYS": AUTH_KEYS,
        "CONSUMER_HOST": "reactor.flow.localhost",
        "CONSUMER_LIMIT": "1024",
        "CONSUMER_PEER_CA_FILE": CA_CERT_PATH,
        "CONSUMER_PORT": "%d" % port,
        "CONSUMER_SERVER_CERT_FILE": TLS_CERT_PATH,
        "CONSUMER_SERVER_CERT_KEY_FILE": TLS_KEY_PATH,
        "DOCKER_DEFAULT_PLATFORM": "linux/amd64",
        "ETCD_ADDRESS": "http://etcd.flow.localhost:2379",
        "FLOW_ALLOW_LOCAL": "true",
        "FLOW_BUILDS_ROOT": FLOW_BUILDS_ROOT,
        "FLOW_CONTROL_API": "http://agent.flow.localhost:8675",
        "FLOW_DASHBOARD": "http://localhost:3000",
        "FLOW_DATA_PLANE_FQDN": "local-cluster.dp.estuary-data.com",
        "FLOW_NETWORK": "supabase_network_flow",
        "LOG_LEVEL": "info",
    },
    links='https://reactor.flow.localhost:9000/debug/pprof',
    resource_deps=['etcd'],
    readiness_probe=probe(
        initial_delay_secs=5,
        http_get=http_get_action(port=port, path='/debug/ready', scheme='https')
    ),
) for port in range(9000, 9001)]

local_resource(
    'agent',
    serve_cmd='%s/flow/.build/package/bin/agent \
    --connector-network supabase_network_flow \
    --allow-local \
    --allow-origin http://localhost:3000 \
    --api-port 8675 \
    --serve-handlers \
    ' % (REPO_BASE),
    serve_env={
        "BIN_DIR": '%s/flow/.build/package/bin' % REPO_BASE,
        "BUILDS_ROOT": FLOW_BUILDS_ROOT,
        "DATABASE_URL": DATABASE_URL,
        "RUST_LOG": "info",
        "SSL_CERT_FILE": CA_CERT_PATH,
        "CONTROL_PLANE_JWT_SECRET": "super-secret-jwt-token-with-at-least-32-characters-long",
    },
    resource_deps=['reactor-9000', 'gazette-8080']
)

local_resource(
    'create-data-plane-local-cluster',
    cmd='sleep 2 && curl -v \
        -X POST \
        -H "content-type: application/json" \
        -H "authorization: bearer %s" \
        --data-binary \'{ \
            "name":"local-cluster",\
            "category": {\
                "manual": {\
                    "brokerAddress": "https://gazette.flow.localhost:8080",\
                    "reactorAddress": "https://reactor.flow.localhost:9000",\
                    "hmacKeys": ["c3VwZXJzZWNyZXQ="]\
                }\
            }\
        }\' http://agent.flow.localhost:8675/admin/create-data-plane' % SYSTEM_USER_TOKEN,
    resource_deps=['agent']
)

local_resource(
    'update-l2-reporting',
    cmd='curl -v \
        -X POST \
        -H "content-type: application/json" \
        -H "authorization: bearer %s" \
        --data-binary \'{ \
            "defaultDataPlane":"ops/dp/public/local-cluster",\
            "dryRun":false\
        }\' http://agent.flow.localhost:8675/admin/update-l2-reporting' % SYSTEM_USER_TOKEN,
    resource_deps=['create-data-plane-local-cluster']
)

local_resource(
    'local-ops-view',
    cmd='./local/ops-publication.sh ops-catalog/local-view.bundle.json | psql "%s"' % DATABASE_URL,
    resource_deps=['update-l2-reporting']
)

local_resource(
    'config-encryption',
    serve_cmd='%s/config-encryption/target/debug/flow-config-encryption --gcp-kms %s' % (REPO_BASE, TEST_KMS_KEY)
)

local_resource(
    'edge-functions',
    serve_cmd='cd %s/flow && supabase functions serve --env-file supabase/env.local --import-map supabase/functions/import-map.json' % REPO_BASE,
    resource_deps=['config-encryption']
)

local_resource(
    'dashboard',
    serve_dir='%s/ui' % REPO_BASE,
    serve_cmd='BROWSER=none npm start',
    links='http://localhost:3000'
)
