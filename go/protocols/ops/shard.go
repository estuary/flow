package ops

import (
	"fmt"
)

func NewShardRef(labeling ShardLabeling) *ShardRef {
	return &ShardRef{
		Name:        labeling.TaskName,
		Kind:        labeling.TaskType,
		KeyBegin:    fmt.Sprintf("%08x", labeling.Range.KeyBegin),
		RClockBegin: fmt.Sprintf("%08x", labeling.Range.RClockBegin),
		Build:       labeling.Build,
	}
}
