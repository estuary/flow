package bindings

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// A podman which records each call, then waits for the test's gate to open
// and finds nothing.
const gatedPodman = `#!/bin/sh
printf '%%s\n' "$*" >>%[1]q/calls
while [ ! -e %[1]q/open ]; do sleep 0.05; done
`

// The reactor's connector proxy service, configured with VmmRecovery, recovers
// what a dead VMM launch left with no shard and no launch. A shard's service
// recovers nothing, and its drop leaves the proxy's pass running. Dropping the
// proxy's service abandons its pass, leaving the record for the next.
func TestTaskServiceV2VmmRecovery(t *testing.T) {
	var dir = t.TempDir()
	var state = filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(state, 0o700))
	var podman = filepath.Join(dir, "podman")
	require.NoError(t, os.WriteFile(podman, []byte(fmt.Sprintf(gatedPodman, dir)), 0o755))

	t.Setenv("CONNECTOR_VMM_IMAGE", "ghcr.io/estuary/connector-vmm:test")
	t.Setenv("CONNECTOR_VMM_PODMAN", podman)
	t.Setenv("CONNECTOR_VMM_STATE_DIR", state)
	t.Setenv("FLOW_DATA_PLANE_FQDN", "dp.example.test")
	t.Setenv("CONSUMER_ZONE", "local")
	t.Setenv("CONSUMER_AUTH_KEYS", base64.StdEncoding.EncodeToString([]byte("a test data-plane key")))

	// The record of a dead launch, which made no directory.
	var plant = func(name, token string) string {
		var mount = filepath.Join(dir, "connector-mounts-1000", "mount-"+name)
		var record = filepath.Join(state, name+".owner")
		var claim = fmt.Sprintf(`{"version":1,"id":%q,"token":%q,"mount":%q}`+"\n", name, token, mount)
		require.NoError(t, os.WriteFile(record, []byte(claim), 0o600))
		return record
	}
	var calls = func() []string {
		var content, err = os.ReadFile(filepath.Join(dir, "calls"))
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		return strings.Split(strings.TrimSpace(string(content)), "\n")
	}
	var gate = filepath.Join(dir, "open")
	var logs = func(ops.Log) {}

	var token = "00000000000000000000000000000001"
	var record = plant("fv_0000000000000001", token)

	var began = time.Now().Unix()
	var shard, err = NewTaskServiceV2(pr.TaskServiceConfig{TaskName: "acmeCo/shard"}, logs)
	require.NoError(t, err)
	t.Cleanup(shard.Drop)
	var proxy *TaskServiceV2
	proxy, err = NewTaskServiceV2(pr.TaskServiceConfig{TaskName: "connector-proxy", VmmRecovery: true}, logs)
	require.NoError(t, err)
	t.Cleanup(proxy.Drop)

	// The proxy's first pass starts at once, and waits on the gate.
	var owned = "ps --all --no-trunc --filter label=dev.estuary.vmm-owner=" + token + " --format {{.ID}}"
	require.Eventually(t, func() bool {
		return len(calls()) == 1 && calls()[0] == owned
	}, 10*time.Second, 10*time.Millisecond, "calls %q", calls())
	shard.Drop()
	require.NoError(t, os.WriteFile(gate, nil, 0o600))

	require.Eventually(t, func() bool {
		var _, err = os.Stat(record)
		return os.IsNotExist(err)
	}, 10*time.Second, 10*time.Millisecond, "the proxy's pass releases the record")
	var labeled = "--filter label=dev.estuary.vmm-state-dir=" + state + " --format"
	require.Eventually(t, func() bool { return len(calls()) == 6 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{
		owned,
		"network ls --filter label=dev.estuary.vmm-owner=" + token + " --format {{.Name}}",
		"volume ls --filter label=dev.estuary.vmm-owner=" + token + " --format {{.Name}}",
		"ps --all " + labeled + " {{.Names}}",
		"network ls " + labeled + " {{.Name}}",
		"volume ls " + labeled + " {{.Name}}",
	}, calls(), "one pass, the proxy's alone")

	require.Eventually(t, func() bool {
		var gathered = gatherVmmRecovery(t)
		return gathered["flow_vmm_recovery_last_success_timestamp_seconds"] >= float64(began) &&
			gathered["flow_vmm_recovery_pending"] == 0
	}, 10*time.Second, 10*time.Millisecond, "the pass's success is exported")
	proxy.Drop()

	// Another proxy's pass waits on the gate, shut again, when it's dropped.
	require.NoError(t, os.Remove(gate))
	require.NoError(t, os.Remove(filepath.Join(dir, "calls")))
	token = "00000000000000000000000000000002"
	record = plant("fv_0000000000000002", token)
	owned = "ps --all --no-trunc --filter label=dev.estuary.vmm-owner=" + token + " --format {{.ID}}"

	proxy, err = NewTaskServiceV2(pr.TaskServiceConfig{TaskName: "connector-proxy", VmmRecovery: true}, logs)
	require.NoError(t, err)
	t.Cleanup(proxy.Drop)
	require.Eventually(t, func() bool {
		return len(calls()) == 1 && calls()[0] == owned
	}, 10*time.Second, 10*time.Millisecond, "calls %q", calls())

	var dropped = time.Now()
	proxy.Drop()
	require.Less(t, time.Since(dropped), 10*time.Second, "the drop waits on no pass")

	var file, openErr = os.Open(record)
	require.NoError(t, openErr, "the record remains for the next pass")
	require.NoError(t, syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB), "and is let go")
	require.NoError(t, file.Close())
}

// gatherVmmRecovery returns the values of this process's VMM recovery metrics.
func gatherVmmRecovery(t *testing.T) map[string]float64 {
	var registry = prometheus.NewRegistry()
	registry.MustRegister(&vmmRecoveryCollector{})
	var families, err = registry.Gather()
	require.NoError(t, err)

	var values = make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			switch {
			case metric.GetGauge() != nil:
				values[family.GetName()] = metric.GetGauge().GetValue()
			case metric.GetCounter() != nil:
				values[family.GetName()] = metric.GetCounter().GetValue()
			}
		}
	}
	return values
}
