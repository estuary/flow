package runtime

import (
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/stretchr/testify/require"
)

func TestIntervalJitterAndDurations(t *testing.T) {
	const period = time.Minute

	for _, tc := range []struct {
		n string
		i time.Duration
	}{{"foo", 35}, {"bar", 52}, {"baz", 0}, {"bing", 39}, {"quip", 56}} {
		require.Equal(t, time.Second*tc.i, intervalJitter(period, tc.n), tc.n)

	}

	require.Equal(t, 20*time.Second, durationToNextInterval(time.Unix(1000, 0), period))
	require.Equal(t, 60*time.Second-100*time.Nanosecond, durationToNextInterval(time.Unix(1020, 100), period))
	require.Equal(t, 1*time.Second, durationToNextInterval(time.Unix(1079, 0), period))
	require.Equal(t, 59*time.Second, durationToNextInterval(time.Unix(1081, 0), period))
}

func TestIntervalStatsShape(t *testing.T) {
	var labels = ops.ShardLabeling{
		TaskName: "some/task",
		Range:    pf.NewFullRange(),
		TaskType: ops.TaskType_capture,
	}

	require.Equal(t,
		`shard:<kind:capture name:"some/task" key_begin:"00000000" r_clock_begin:"00000000" > timestamp:<seconds:1600000000 > interval:<uptime_seconds:300 usage_rate:1 > `,
		intervalStats(time.Unix(1600000000, 0), 5*time.Minute, labels).String())

	labels.TaskType = ops.TaskType_derivation

	require.Equal(t,
		`shard:<kind:derivation name:"some/task" key_begin:"00000000" r_clock_begin:"00000000" > timestamp:<seconds:1500000000 > interval:<uptime_seconds:600 > `,
		intervalStats(time.Unix(1500000000, 0), 10*time.Minute, labels).String())
}
