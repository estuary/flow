package shuffle

import (
	"testing"
)

func TestFoobar(t *testing.T) {
	_ = Config{
		Processors: []Config_Processor{
			{MinMsgClock: 0, MaxMsgClock: 0},
			{MinMsgClock: 0, MaxMsgClock: 0},
			{MinMsgClock: 0, MaxMsgClock: 0},
			{MinMsgClock: 1000, MaxMsgClock: 0},
			{MinMsgClock: 0, MaxMsgClock: 2000},
		},
		BroadcastTo: 3,
		ChooseFrom:  1,
	}

}
