package message

import (
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

type SimplePublisherSuite struct{}

func (s *SimplePublisherSuite) TestPublish(c *gc.C) {
	var topic = &topic.Description{
		Name:       "a/topic",
		Partitions: 4,
		RoutingKey: func(m interface{}) string { return string(m.(msgStr)) },
	}
	var writer = &journal.MockWriter{}

	// Expect "test-message" to be framed and written to part-003.
	writer.On("Write", journal.Name("a/topic/part-003"),
		mock.AnythingOfType("[]uint8")).Return(new(journal.AsyncAppend), nil).Once()

	c.Check(SimplePublisher{writer}.Publish(msgStr("test-message"), topic), gc.IsNil)

	var recovered msgStr
	c.Check(ParseExactFrame(&recovered,
		writer.Calls[0].Arguments.Get(1).([]byte)), gc.IsNil)
	c.Check(recovered, gc.Equals, msgStr("test-message"))
}

// string as topic.Marshallable / Unmarshallable.
type msgStr string

func (m msgStr) MarshalTo(buf []byte) (int, error) { return copy(buf, m), nil }
func (m msgStr) Size() int                         { return len(m) }
func (m *msgStr) Unmarshal(buf []byte) error       { *m = msgStr(buf); return nil }

var _ = gc.Suite(&SimplePublisherSuite{})
