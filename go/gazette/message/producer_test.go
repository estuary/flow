package message

import (
	"bytes"
	"io/ioutil"

	//log "github.com/Sirupsen/logrus"
	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
)

// TODO(johnny): Add additional tests on error handling and recovery.
// Also add tests on pump behavior.
type ProducerSuite struct {
	message    []byte
	message3x  []byte // Repeats |message| 3 times.
	messageLen int64
}

func (s *ProducerSuite) SetUpTest(c *gc.C) {
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

	// Expect a second read, updated from the result offset.
	getter.On("Get", journal.ReadArgs{
		Journal: "foo", Offset: 1234 + s.messageLen, Blocking: true}).
		Return(journal.ReadResult{Offset: s.messageLen},
		ioutil.NopCloser(bytes.NewReader(s.message))).Once()

	producer := NewProducer(getter, func() Unmarshallable {
		var m frameablestring
		return &m
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	producer.Pump()
	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(1234))
	producer.Cancel()

	getter.AssertExpectations(c)
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
		ioutil.NopCloser(bytes.NewReader(s.message3x[msgLen:])), nil).Once()

	producer := NewProducer(getter, func() Unmarshallable {
		var m frameablestring
		return &m
	})
	recovered := make(chan Message)
	producer.StartProducingInto(journal.NewMark("foo", 0), recovered)

	// Expect to pop three framed messages.
	producer.Pump()
	c.Check((<-recovered).Mark.Offset, gc.Equals, int64(0))

	producer.Pump()
	c.Check((<-recovered).Mark.Offset, gc.Equals, msgLen)

	producer.Pump()
	producer.Cancel() // Producer will now exit after next pop.
	c.Check((<-recovered).Mark.Offset, gc.Equals, 2*msgLen)
}

var _ = gc.Suite(&ProducerSuite{})
