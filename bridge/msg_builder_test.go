package bridge

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestBuilder(t *testing.T) {
	var b = NewMsgBuilder()

	var specA = &pb.JournalSpec{
		LabelSet: pb.MustLabelSet(UUIDLabel, "/_meta/uuid"),
	}
	var specB = &pb.JournalSpec{
		LabelSet: pb.MustLabelSet(UUIDLabel, "/_uuid"),
	}
	var fixture = "7367f4f3-7668-4370-b06f-021c828d6ed8"

	var tst = func(s *pb.JournalSpec, expect string) {
		var msg, err = b.Build(s)
		assert.NoError(t, err)
		msg.SetUUID(uuid.MustParse(fixture))
		assert.Equal(t, expect, string(msg.(Message).AppendJSONTo(nil)))
		msg.(Message).Drop()
	}

	tst(specA, `{"_meta":{"uuid":"`+fixture+`"}}`+"\n")
	tst(specB, `{"_uuid":"`+fixture+`"}`+"\n")
	tst(specA, `{"_meta":{"uuid":"`+fixture+`"}}`+"\n")

	b.PurgeCache()

	tst(specB, `{"_uuid":"`+fixture+`"}`+"\n")
	tst(specA, `{"_meta":{"uuid":"`+fixture+`"}}`+"\n")
	tst(specB, `{"_uuid":"`+fixture+`"}`+"\n")
}
