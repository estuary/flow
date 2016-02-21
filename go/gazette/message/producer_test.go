package message

import (
	"bytes"
	"io"
	"time"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

type ProducerSuite struct {
	message    []byte
	message3x  []byte // Repeats |message| 3 times.
	messageLen int64
}

func (s *ProducerSuite) SetUpSuite(c *gc.C) {
	c.Check(Frame(frameablestring("foobar"), &s.message), gc.IsNil)
	s.message3x = bytes.Repeat(s.message, 3)
	s.messageLen = int64(len(s.message))
}

func (s *ProducerSuite) TestBasicProduction(c *gc.C) {
	reader := struct {
		io.Reader
		closeCh
	}{bytes.NewReader(s.message3x), make(closeCh)}

	// Return a result fixture which skips forward from 0 => 1234.
	getter := &journal.MockGetter{}
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 0, Blocking: true}).
		Return(journal.ReadResult{Offset: 1234}, reader).Once()

	producer := NewProducer(getter, &topic.Description{
		GetMessage: func() topic.Unmarshallable {
			var m frameablestring
			return &m
		},
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(1234)+1*s.messageLen)
	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(1234)+2*s.messageLen)
	producer.Cancel()

	// TODO(johnny): Give producer loop time to process the cancel. This is ugly.
	// Living with it for now, as Producer is deprecated by V2 consumers.
	time.Sleep(10 * time.Millisecond)

	// Expect reader.Close() to have been called (regression test for issue #890).
	<-reader.closeCh
}

type closeCh chan struct{}

func (c closeCh) Close() error { close(c); return nil }

var _ = gc.Suite(&ProducerSuite{})
