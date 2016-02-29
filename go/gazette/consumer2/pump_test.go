package consumer

import (
	"bytes"
	"io"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

type PumpSuite struct{}

func (s *PumpSuite) TestPump(c *gc.C) {
	var buffer []byte
	c.Check(message.Frame(msgStr("foobar"), &buffer), gc.IsNil)

	reader := struct {
		io.Reader
		closeCh
	}{bytes.NewReader(bytes.Repeat(buffer, 3)), make(closeCh)}

	// Return a result fixture which skips forward from 0 => 1234.
	var getter = &journal.MockGetter{}
	getter.On("Get", journal.ReadArgs{Journal: "a/journal", Offset: 0, Blocking: true}).
		Return(journal.ReadResult{Offset: 1234}, reader).Once()

	var topic = &topic.Description{
		GetMessage: func() topic.Unmarshallable {
			var m msgStr
			return &m
		},
	}

	var msgCh = make(chan message.Message)
	var cancelCh = make(chan struct{})

	go newPump(getter, msgCh, cancelCh).pump(topic, journal.NewMark("a/journal", 0))

	// Read two messages. Expect the topic and next journal mark accompany it.
	msg := <-msgCh
	c.Check(msg.Mark, gc.Equals, journal.NewMark("a/journal", int64(1234+1*len(buffer))))
	c.Check(msg.Topic, gc.Equals, topic)
	c.Check(*msg.Value.(*msgStr), gc.Equals, msgStr("foobar"))

	msg = <-msgCh
	c.Check(msg.Mark, gc.Equals, journal.NewMark("a/journal", int64(1234+2*len(buffer))))
	c.Check(msg.Topic, gc.Equals, topic)
	c.Check(*msg.Value.(*msgStr), gc.Equals, msgStr("foobar"))

	// After closing |cancelCh|, expect that pump exited closing |reader|.
	close(cancelCh)
	<-reader.closeCh
}

// string as topic.Marshallable / Unmarshallable.
type msgStr string

func (m msgStr) MarshalTo(buf []byte) (int, error) { return copy(buf, m), nil }
func (m msgStr) Size() int                         { return len(m) }
func (m *msgStr) Unmarshal(buf []byte) error       { *m = msgStr(buf); return nil }

// io.Closer which closes its channel.
type closeCh chan struct{}

func (c closeCh) Close() error { close(c); return nil }

var _ = gc.Suite(&PumpSuite{})
