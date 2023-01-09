package labels

import (
	"fmt"
	"strconv"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

type PortConfig struct {
	ContainerPort uint16
	AlpnProtocol  string
}

// ShardLabeling is a parsed and validated representation of the Flow
// labels which are attached to Gazette ShardSpecs, that are understood
// by the Flow runtime and influence its behavior with respect to the shard.
type ShardLabeling struct {
	// Catalog build identifier which the task uses.
	Build string
	// Logging level of the task.
	LogLevel pf.LogLevel
	// Key and R-Clock range of the shard.
	Range pf.RangeSpec
	// If non-empty, the shard which this task is splitting from.
	SplitSource string `json:",omitempty"`
	// If non-empty, the shard which this task is splitting into.
	SplitTarget string `json:",omitempty"`
	// Name of the shard's task.
	TaskName string
	// Type of this task (capture, derivation, or materialization).
	TaskType string

	// Ports is a map from port name to the combined configuration
	// for the port. The runtime itself doesn't actually care
	// about the alpn protocol, but it's there for the sake of
	// completeness.
	Ports map[string]*PortConfig `json:",omitempty"`
}

// ParseShardLabels parses and validates ShardLabels from their defined
// label names, and returns any encountered error in the representation.
func ParseShardLabels(set pf.LabelSet) (ShardLabeling, error) {
	var out ShardLabeling
	var err error

	if levelStr, err := ExpectOne(set, LogLevel); err != nil {
		return out, err
	} else if mapped, ok := pf.LogLevel_value[levelStr]; !ok {
		return out, fmt.Errorf("%q is not a valid log level", levelStr)
	} else {
		out.LogLevel = pf.LogLevel(mapped)
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
	if out.TaskType, err = ExpectOne(set, TaskType); err != nil {
		return out, err
	}

	switch out.TaskType {
	case TaskTypeCapture, TaskTypeDerivation, TaskTypeMaterialization:
		// Pass.
	default:
		return out, fmt.Errorf("unknown task type %q", out.TaskType)
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

func parsePorts(set pf.LabelSet) (map[string]*PortConfig, error) {
	var out = make(map[string]*PortConfig)
	for _, label := range set.Labels {
		if strings.HasPrefix(label.Name, PortPrefix) {
			var portName = label.Name[len(PortPrefix):]
			if _, ok := out[portName]; !ok {
				out[portName] = &PortConfig{}
			}

			var port, err = strconv.ParseUint(label.Value, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("invalid value for '%s': '%s'", label.Name, label.Value)
			}
			out[portName].ContainerPort = uint16(port)
		}
		if strings.HasPrefix(label.Name, PortProtoPrefix) {
			var portName = label.Name[len(PortProtoPrefix):]
			if _, ok := out[portName]; !ok {
				out[portName] = &PortConfig{}
			}
			out[portName].AlpnProtocol = label.Value
		}
	}
	return out, nil
}
