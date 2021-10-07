package labels

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/labels"
)

func TestRuntimeLabels(t *testing.T) {
	var cases = []struct {
		label  string
		expect bool
	}{
		{"other", false},
		{Collection, false},
		{FieldPrefix + "One", true},
		{FieldPrefix + "two", true},
		{KeyBegin, true},
		{KeyEnd, true},
		{LogLevel, false},
		{RClockBegin, true},
		{RClockEnd, true},
		{SplitSource, true},
		{SplitTarget, true},
		{TaskCreated, true},
		{TaskName, false},
		{TaskType, false},
		{labels.ContentType, false},
		{labels.Instance, false},
		{labels.ManagedBy, false},
		{labels.MessageSubType, false},
		{labels.MessageType, false},
		{labels.Region, false},
		{labels.Tag, false},
	}
	for _, c := range cases {
		require.Equal(t, IsRuntimeLabel(c.label), c.expect)
	}
}
