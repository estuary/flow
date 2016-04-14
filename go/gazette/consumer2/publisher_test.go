package consumer

import (
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

type PublisherSuite struct{}

func (s *PublisherSuite) TestPublish(c *gc.C) {
	var topic = &topic.Description{
		Name:       "a/topic",
		Partitions: 4,
		RoutingKey: func(m interface{}) string { return string(m.(msgStr)) },
	}
	var writer = &journal.MockWriter{}

	// Expect "test-message" to be framed and written to part-003.
	writer.On("Write", journal.Name("a/topic/part-003"),
		mock.AnythingOfType("[]uint8")).Return(new(journal.AsyncAppend), nil).Once()

	c.Check(publisher{writer}.Publish(msgStr("test-message"), topic), gc.IsNil)

	var recovered msgStr
	c.Check(message.ParseExactFrame(&recovered,
		writer.Calls[0].Arguments.Get(1).([]byte)), gc.IsNil)
	c.Check(recovered, gc.Equals, msgStr("test-message"))
}

var _ = gc.Suite(&PublisherSuite{})
