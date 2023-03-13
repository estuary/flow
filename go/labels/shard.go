package labels

import (
	"fmt"
	"strconv"

	pf "github.com/estuary/flow/go/protocols/flow"
)

type PortConfig struct {
	Protocol string
	Public   bool
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

	// TODO: comment and add test for parsing host and ports
	Hostname string

	// Ports is a map from port name to the combined configuration
	// for the port. The runtime itself doesn't actually care
	// about the alpn protocol, but it's there for the sake of
	// completeness.
	Ports map[uint16]*PortConfig `json:",omitempty"`
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
	if out.Ports, err = parsePorts(set); err != nil {
		return out, err
	}
	if out.Hostname, err = maybeOne(set, Hostname); err != nil {
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

func parsePorts(set pf.LabelSet) (map[uint16]*PortConfig, error) {
	var out = make(map[uint16]*PortConfig)

	for _, value := range set.ValuesOf(ExposePort) {
		var portNumber, err = strconv.ParseUint(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("parsing value '%s' of label '%s': %w", value, ExposePort, err)
		}
		if portNumber == 0 || portNumber > 65535 {
			return nil, fmt.Errorf("invalid '%s' value: '%s'", ExposePort, value)
		}

		var config = new(PortConfig)
		config.Protocol = set.ValueOf(PortProtoPrefix + value)
		if publicVal := set.ValueOf(PortPublicPrefix + value); publicVal != "" {
			config.Public, err = strconv.ParseBool(publicVal)
			if err != nil {
				return nil, fmt.Errorf("parsing '%s=%s': %w", PortPublicPrefix, publicVal, err)
			}
		}
		out[uint16(portNumber)] = config
	}
	return out, nil
}
