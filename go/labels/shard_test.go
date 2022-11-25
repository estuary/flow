package labels

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestParsingShardLabels(t *testing.T) {
	var set = pb.MustLabelSet(
		Build, "a-build",
		LogLevel, "debug",
		KeyBegin, "aaaaaaaa",
		KeyEnd, "bbbbbbbb",
		RClockBegin, "cccccccc",
		RClockEnd, "dddddddd",
		TaskName, "a-task",
		TaskType, TaskTypeCapture,
		SplitSource, "a-source",
	)
	var out, err = ParseShardLabels(set)
	require.NoError(t, err)

	require.Equal(t, ShardLabeling{
		Build:    "a-build",
		LogLevel: pf.LogLevel_debug,
		Range: pf.RangeSpec{
			KeyBegin:    0xaaaaaaaa,
			KeyEnd:      0xbbbbbbbb,
			RClockBegin: 0xcccccccc,
			RClockEnd:   0xdddddddd,
		},
		SplitSource: "a-source",
		SplitTarget: "",
		TaskName:    "a-task",
		TaskType:    TaskTypeCapture,
	}, out)

	// Case: invalid log-level.
	set.SetValue(LogLevel, "whoops")
	_, err = ParseShardLabels(set)
	require.EqualError(t, err, "\"whoops\" is not a valid log level")
	set.SetValue(LogLevel, "warn")

	// Case: swap SplitSource/Target
	set.Remove(SplitSource)
	set.SetValue(SplitTarget, "a-target")

	out, err = ParseShardLabels(set)
	require.NoError(t, err)
	require.Equal(t, "a-target", out.SplitTarget)

	// Case: both SplitSource and Target.
	set.SetValue(SplitSource, "a-source")
	_, err = ParseShardLabels(set)
	require.EqualError(t, err,
		"both split-source \"a-source\" and split-target \"a-target\" are set but shouldn't be")
	set.Remove(SplitSource)

	// Case: invalid task type
	set.SetValue(TaskType, "whoops")
	_, err = ParseShardLabels(set)
	require.EqualError(t, err, "unknown task type \"whoops\"")

	// Case: empty label (expectOne).
	set.SetValue(TaskType, "")
	_, err = ParseShardLabels(set)
	require.EqualError(t, err, "label \"estuary.dev/task-type\" value is empty but shouldn't be")

	// Case: too many / few label values (expectOne).
	set.SetValue(TaskType, TaskTypeCapture)
	set.AddValue(TaskType, TaskTypeDerivation)

	_, err = ParseShardLabels(set)
	require.Regexp(t, "expected one label .* \\(got \\[capture derivation\\]\\)", err.Error())

	set.Remove(TaskType)
	_, err = ParseShardLabels(set)
	require.Regexp(t, "expected one label .* \\(got \\[\\]\\)", err.Error())
	set.SetValue(TaskType, TaskTypeCapture)

	// Case: empty label (maybeOne).
	set.SetValue(SplitSource, "")

	_, err = ParseShardLabels(set)
	require.EqualError(t, err, "label \"estuary.dev/split-source\" value is empty but shouldn't be")

	// Case: to many label values (maybeOne).
	set.SetValue(SplitSource, "a-source")
	set.AddValue(SplitSource, "source-2")
	_, err = ParseShardLabels(set)
	require.Regexp(t, "expected one label .* \\(got \\[a-source source-2\\]\\)", err.Error())
	set.Remove(SplitSource)

	// Case: range parse error is passed through.
	set.SetValue(KeyBegin, "whoops")
	_, err = ParseShardLabels(set)
	require.EqualError(t, err,
		"expected estuary.dev/key-begin to be a 4-byte, hex encoded integer; got whoops")
}
