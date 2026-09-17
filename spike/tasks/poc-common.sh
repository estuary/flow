# Constants and helpers shared by the POC scripts. Sourced, never executed.
#
# Unlike the experiment scripts, these drive a live local stack, so they need
# the per-stack ambient environment that mise/tasks/local/stack-env provides
# (FLOW_CLUSTER, FLOW_PG_URL, FLOW_PLANE_BASE, ...) as well as the spike's own
# names. Everything runs under mise:
#
#     mise exec -- spike/tasks/poc-up.sh

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env-common.sh"

for _v in FLOW_STACK_NAME FLOW_CLUSTER FLOW_PLANE_BASE FLOW_PG_URL \
          FLOW_STACK_DIR CARGO_TARGET_DIR; do
    if [ -z "${!_v:-}" ]; then
        echo "$_v is unset; run under mise, e.g. mise exec -- spike/tasks/poc-up.sh" >&2
        return 2
    fi
done
unset _v

# mise/tasks/local/data-plane allocates reactors counting down from base+99, and
# the stack asks for exactly one.
POC_DP="$FLOW_CLUSTER"
POC_REACTOR_PORT=$((FLOW_PLANE_BASE + 99))
POC_USER_UNIT="flow-reactor@${POC_DP}-${POC_REACTOR_PORT}.service"
POC_ROOT_UNIT=flow-reactor-sandbox.service
POC_ROOT_UNIT_PATH="/etc/systemd/system/${POC_ROOT_UNIT}"
# The sidecar has to move to root alongside the reactor. A V2 derivation shard
# creates its shuffle directory with `os.MkdirTemp` (go/runtime/derive_v2.go),
# which is mode 0700 owned by the reactor, and then the sidecar writes log
# segments into it. Split the two uids and every derivation dies with
# "failed to create log segment ... Permission denied". Captures do not shuffle,
# which is why they survive the split and derivations do not.
POC_SIDECAR_USER_UNIT="flow-runtime-sidecar@${POC_DP}.service"
POC_SIDECAR_ROOT_UNIT=flow-runtime-sidecar-sandbox.service
POC_SIDECAR_ROOT_UNIT_PATH="/etc/systemd/system/${POC_SIDECAR_ROOT_UNIT}"
POC_SIDECAR_ENV="$HOME/flow-local/env/runtime-sidecar-${POC_DP}.env"
POC_AGENT_UNIT="flow-control-agent@${FLOW_STACK_NAME}.service"
POC_AGENT_DROPIN="$HOME/.config/systemd/user/${POC_AGENT_UNIT}.d/poc.conf"
POC_REACTOR_ENV="$HOME/flow-local/env/reactor-${POC_DP}-${POC_REACTOR_PORT}.env"
POC_SANDBOX_ENV="$HOME/flow-local/env/reactor-sandbox.env"

POC_TENANT=acmeCo
POC_TENANT_ENV="$FLOW_STACK_DIR/test-tenant-${POC_TENANT}.env"
POC_WORK_DIR="$HOME/poc"
POC_CAPTURE="$POC_TENANT/hello-world"
POC_DERIVATION="$POC_TENANT/pandas-rollup"
POC_POLICY=policy-allow-all.json
POC_FLOWCTL="$CARGO_TARGET_DIR/debug/flowctl"

# How long G6 leaves the derivation with no input. The value that matters is
# "longer than anything the keep-alive machinery was ever exercised over", not
# this particular number; override to shorten a debugging run.
: "${POC_IDLE_SECS:=600}"

# Every ops task the stack puts on this reactor, chosen by predicate rather than
# by name: the L1 rollups are created per data-plane, so their names carry the
# cluster name and a hardcoded list goes stale. A task is a materialization or a
# collection with a `derive`; the bare `ops/tasks/.../logs` and `.../stats`
# collections have no shards and must be excluded, since the patch below would
# reduce them to NULL.
POC_OPS_PREDICATE="(ls.catalog_name like 'ops/%' or ls.catalog_name like 'ops.%')
      and (ls.spec_type = 'materialization' or ls.spec::jsonb ? 'derive')"

step() { printf '\n== %s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; POC_FAILURES=$((${POC_FAILURES:-0} + 1)); }

# -q suppresses command tags so a trailing `select` is the only thing on stdout.
poc_psql() { psql -qAt -v ON_ERROR_STOP=1 "$FLOW_PG_URL" "$@"; }

# flowctl as the POC tenant. FLOWCTL_PROFILE is ambient under mise; the tenant
# env file carries FLOW_AUTH_TOKEN and already exports it. SSL_CERT_FILE is not
# ambient and must be set per-command (local/README.md, and `local:stack-info`
# prints it): broker and reactor endpoints present the stack's own CA, so
# without it every journal read fails with `invalid peer certificate:
# UnknownIssuer` -- which reads like an empty collection, not a trust problem.
poc_flowctl() {
    (
        . "$POC_TENANT_ENV"
        export SSL_CERT_FILE="$HOME/flow-local/ca.crt"
        "$POC_FLOWCTL" "$@"
    )
}

# Publish $1 (a basename under $POC_WORK_DIR, default the root spec). Naming a
# single spec matters for the capture: a publication only touches what the draft
# contains, so toggling the capture does not re-run the derivation's Validate,
# which is an image pull plus a uv resolve plus pyright every time.
poc_publish() {
    poc_flowctl catalog publish --auto-approve \
        --source "$POC_WORK_DIR/${1:-flow.yaml}"
}

# Name of the helper (or plain connector) container serving task $1, or empty.
# `index` is required: a Go template cannot spell a key containing a hyphen as a
# field, and `{{.Labels.task-name}}` is a parse error rather than a miss.
poc_container() {
    sudo podman ps --filter "label=task-name=$1" --format '{{.Names}}' | head -1
}

# The bridge the helpers sit on, which the rendered ruleset drops as part of its
# baseline. Read from podman rather than hardcoded: it is netavark's to choose.
poc_bridge_subnet() {
    sudo podman network inspect "$SPIKE_NET_CONNECTORS" \
        --format '{{range .Subnets}}{{.Subnet}}{{end}}'
}

# Block until CMD exits zero, up to $1 seconds.
poc_wait() {
    local secs="$1" deadline
    shift
    deadline=$((SECONDS + secs))
    while [ "$SECONDS" -lt "$deadline" ]; do
        "$@" >/dev/null 2>&1 && return 0
        sleep 2
    done
    return 1
}

poc_has_container() { [ -n "$(poc_container "$1")" ]; }

# Republish the six ops tasks with `shards.disable` set to $1 (true|false) as
# the ops user, and block until the publication leaves `queued`.
#
# `shards` hangs off the task, which for a derivation means `derive.shards`.
# The value is merged rather than written by path because an empty `shards` is
# omitted from the stored spec entirely, leaving no object for jsonb_set to
# land in. The publication id comes back through a temp table: it is generated
# inside the DO block, and psql holds one session across the whole heredoc.
poc_ops_disable() {
    local want="$1" id status
    id=$(poc_psql <<SQL
create temporary table _poc_pub (id flowid);
do \$\$
declare
    ops_user_id uuid;
    new_draft_id flowid := internal.id_generator();
    publication_id flowid := internal.id_generator();
begin
    select id into strict ops_user_id from auth.users
        where email = 'support@estuary.dev';

    insert into drafts (id, user_id, detail) values
        (new_draft_id, ops_user_id, 'POC: ops shards disable=${want}');
    insert into publications (id, user_id, draft_id, data_plane_name) values
        (publication_id, ops_user_id, new_draft_id, 'ops/dp/public/${POC_DP}');

    insert into draft_specs (draft_id, catalog_name, spec_type, spec)
    select new_draft_id, ls.catalog_name, ls.spec_type,
      case ls.spec_type
        when 'materialization' then (ls.spec::jsonb || jsonb_build_object(
          'shards', coalesce(ls.spec::jsonb->'shards', '{}'::jsonb)
                    || '{"disable":${want}}'::jsonb))::json
        when 'collection' then (ls.spec::jsonb || jsonb_build_object(
          'derive', ls.spec::jsonb->'derive' || jsonb_build_object(
            'shards', coalesce(ls.spec::jsonb->'derive'->'shards', '{}'::jsonb)
                      || '{"disable":${want}}'::jsonb)))::json
      end
    from live_specs ls where ${POC_OPS_PREDICATE};

    insert into _poc_pub values (publication_id);
end \$\$ language plpgsql;
select id from _poc_pub;
SQL
    )

    for _ in $(seq 1 150); do
        status=$(poc_psql -c "select job_status->>'type' from publications where id = '$id'")
        [ "$status" = queued ] || break
        sleep 2
    done
    # `emptyDraft` is what a no-op publication returns: prune_unchanged_draft_specs
    # empties the draft when every spec already carries the value we asked for, so
    # re-running against an already-disabled stack lands here, not on success.
    case "$status" in
    success | emptyDraft) ;;
    *)
        echo "ops publication $id ended '$status':" >&2
        poc_psql -c "select job_status from publications where id = '$id'" >&2
        return 1
        ;;
    esac
    echo "ops tasks: shards.disable=${want} ($status, publication $id)"
}

# Flip the capture's `shards.disable` to $1 and republish. G6 needs the
# derivation to see no input for a while, and stopping its source is the only
# way to arrange that without touching the module under test.
poc_capture_disable() {
    python3 - "$POC_WORK_DIR/capture-hello-world.flow.yaml" "$1" <<'PY'
import sys, yaml
path, want = sys.argv[1], sys.argv[2] == 'true'
doc = yaml.safe_load(open(path))
next(iter(doc['captures'].values()))['shards']['disable'] = want
yaml.safe_dump(doc, open(path, 'w'), sort_keys=False)
PY
    poc_publish capture-hello-world.flow.yaml
}
