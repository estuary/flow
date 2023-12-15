package labels

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
)

// ParseShardLabels parses and validates ShardLabels from their defined
// label names, and returns any encountered error in the representation.
func ParseShardLabels(set pf.LabelSet) (ops.ShardLabeling, error) {
	var out ops.ShardLabeling
	var err error

	if levelStr, err := ExpectOne(set, LogLevel); err != nil {
		return out, err
	} else if mapped, ok := ops.Log_Level_value[levelStr]; !ok {
		return out, fmt.Errorf("%q is not a valid log level", levelStr)
	} else {
		out.LogLevel = ops.Log_Level(mapped)
	}
	if out.Range, err = ParseRangeSpec(set); err != nil {
		return out, err
	}
	if out.SplitSource, err = maybeOne(set, SplitSource); err != nil {
		return out, err
	}
	if out.SplitTarget, err = maybeOne(set, SplitTarget); err != nil {
		return out, err
	}
	if out.Build, err = ExpectOne(set, Build); err != nil {
		return out, err
	}
	if out.TaskName, err = ExpectOne(set, TaskName); err != nil {
		return out, err
	}

	taskType, err := ExpectOne(set, TaskType)
	if err != nil {
		return out, err
	} else if kind, ok := ops.TaskType_value[taskType]; !ok {
		return out, fmt.Errorf("unknown task type %q", taskType)
	} else {
		out.TaskType = ops.TaskType(kind)
	}
	if out.Hostname, err = maybeOne(set, Hostname); err != nil {
		return out, err
	}

	if out.SplitSource != "" && out.SplitTarget != "" {
		return out, fmt.Errorf(
			"both split-source %q and split-target %q are set but shouldn't be",
			out.SplitSource, out.SplitTarget)
	}

	return out, nil
}

// ExpectOne extracts label |name| from the |set|.
// The label is expected to exist with a single non-empty value.
func ExpectOne(set pf.LabelSet, name string) (string, error) {
	if v := set.ValuesOf(name); len(v) != 1 {
		return "", fmt.Errorf("expected one label for %q (got %v)", name, v)
	} else if len(v[0]) == 0 {
		return "", fmt.Errorf("label %q value is empty but shouldn't be", name)
	} else {
		return v[0], nil
	}
}

func maybeOne(set pf.LabelSet, name string) (string, error) {
	if v := set.ValuesOf(name); len(v) > 1 {
		return "", fmt.Errorf("expected one label for %q (got %v)", name, v)
	} else if len(v) == 0 {
		return "", nil
	} else if len(v[0]) == 0 {
		return "", fmt.Errorf("label %q value is empty but shouldn't be", name)
	} else {
		return v[0], nil
	}
}
