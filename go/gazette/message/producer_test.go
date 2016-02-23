package message

import (
	"bytes"
	"io"
	"io/ioutil"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
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

func (s *ProducerSuite) TestOffsetUpdatesFromReadResult(c *gc.C) {
	getter := &journal.MockGetter{}

	// Return a result fixture which skips forward from 0 => 1234.
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 0, Blocking: true}).
		Return(journal.ReadResult{Offset: 1234},
		ioutil.NopCloser(bytes.NewReader(s.message))).Once()

	// Next reads fail. Note this may not be called (depending on Cancel() ordering).
	getter.On("Get", journal.ReadArgs{Journal: "foo",
		Offset: 1234 + s.messageLen, Blocking: true}).
		Return(journal.ReadResult{Error: journal.ErrNotBroker}, ioutil.NopCloser(nil))

	producer := NewProducer(getter, func() Unmarshallable {
		var m frameablestring
		return &m
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(1234))
	producer.Cancel()
}

func (s *ProducerSuite) TestCorrectMarkIsUsedOnReOpen(c *gc.C) {
	msgLen := int64(len(s.message))

	// On first open, return a reader which returns 1.5 messages before EOF.
	getter := &journal.MockGetter{}
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 0, Blocking: true}).
		Return(journal.ReadResult{Offset: 0},
		ioutil.NopCloser(bytes.NewReader(s.message3x[:msgLen*3/2]))).Once()

	// Expect the next open is from a Mark at the first message boundary,
	// and not from the maximum forward progress of the previous reader.
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: msgLen, Blocking: true}).
		Return(journal.ReadResult{Offset: msgLen},
		ioutil.NopCloser(bytes.NewReader(s.message3x[msgLen:]))).Once()

	// Next reads fail. Note this may not be called (depending on Cancel() ordering).
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 3 * msgLen, Blocking: true}).
		Return(journal.ReadResult{Error: journal.ErrNotBroker}, ioutil.NopCloser(nil))

	producer := NewProducer(getter, func() Unmarshallable {
		var m frameablestring
		return &m
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	// Expect to pop three framed messages.
	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(0))
	c.Check((<-recovered).Mark.Offset, gc.Equals, msgLen)
	c.Check((<-recovered).Mark.Offset, gc.Equals, 2*msgLen)

	producer.Cancel()
}

func (s *ProducerSuite) TestOffsetUpdatesMidStream(c *gc.C) {
	msgLen := int64(len(s.message))

	getter := &journal.MockGetter{}
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 0, Blocking: true}).
		Return(journal.ReadResult{Offset: 0},
		ioutil.NopCloser(bytes.NewReader(s.message))).Once()

	// Next read skips the resulting offset forward.
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: msgLen, Blocking: true}).
		Return(journal.ReadResult{Offset: 2 * msgLen},
		ioutil.NopCloser(bytes.NewReader(s.message))).Once()

	// Next reads fail.
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 3 * msgLen, Blocking: true}).
		Return(journal.ReadResult{Error: journal.ErrNotBroker}, ioutil.NopCloser(nil))

	producer := NewProducer(getter, func() Unmarshallable {
		var m frameablestring
		return &m
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	// Expect to pop two framed messages with correct offsets.
	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(0))
	c.Check((<-recovered).Mark.Offset, gc.Equals, 2*msgLen)

	producer.Cancel()
}

// Regression test for issue #890.
func (s *ProducerSuite) TestReaderIsClosedOnCancel(c *gc.C) {
	reader := struct {
		io.Reader
		closeCh
	}{bytes.NewReader(s.message3x), make(closeCh)}

	// Return a result fixture which expects to be Close()'d prematurely.
	getter := &journal.MockGetter{}
	getter.On("Get", journal.ReadArgs{Journal: "foo", Offset: 0, Blocking: true}).
		Return(journal.ReadResult{Offset: 0}, reader).Once()

	producer := NewProducer(getter, func() Unmarshallable {
		var m frameablestring
		return &m
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	<-recovered

	producer.Cancel()
	<-reader.closeCh // Expect Close() to have been called.
	getter.AssertExpectations(c)
}

type closeCh chan struct{}

func (c closeCh) Close() error {
	close(c)
	return nil
}

var _ = gc.Suite(&ProducerSuite{})
