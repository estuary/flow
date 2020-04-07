package shuffle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidation(t *testing.T) {
	var spec = Config{
		ChooseFrom:    123,
		BroadcastTo:   456,
		UuidJsonPtr:   "/ptr",
		ShuffleKeyPtr: []string{"/ptr"},
	}

	assert.EqualError(t, spec.Validate(), "expected at least one Processor")
	spec.Processors = []Config_Processor{
		{MinMsgClock: 1},
		{MinMsgClock: 20, MaxMsgClock: 10},
	}

	assert.EqualError(t, spec.Validate(), "Processors[0] cannot have clock bounds (min_msg_clock:1 )")
	spec.Processors[0] = Config_Processor{}

	assert.EqualError(t, spec.Validate(), "Processors[1]: invalid min/max clocks (min clock 20 > max 10)")
	spec.Processors[1].MaxMsgClock = 30

	assert.EqualError(t, spec.Validate(), "expected one of ChooseFrom or BroadcastTo to be non-zero")
	spec.ChooseFrom, spec.BroadcastTo = 0, 0
	assert.EqualError(t, spec.Validate(), "expected one of ChooseFrom or BroadcastTo to be non-zero")
	spec.ChooseFrom = 1

	assert.NoError(t, spec.Validate())
}
