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
type ProducerSuite struct{}

func (s *ProducerSuite) TestCorrectMarkIsUsedOnReOpen(c *gc.C) {
	// Produce a buffer with three framed messages.
	var buf []byte
	c.Check(Frame(frameablestring("foobar"), &buf), gc.IsNil)
	msgLen := int64(len(buf))
	buf = bytes.Repeat(buf, 3)

	// On first open, return a reader which returns 1.5 messages before EOF.
	opener := &journal.MockOpener{}
	opener.On("OpenJournalAt", journal.NewMark("foo", 0)).Return(
		ioutil.NopCloser(bytes.NewReader(buf[:msgLen*3/2])), nil).Once()
	// Expect the next open is from a Mark at the first message boundary,
	// and not from the maximum forward progress of the previous reader.
	opener.On("OpenJournalAt", journal.NewMark("foo", msgLen)).Return(
		ioutil.NopCloser(bytes.NewReader(buf[msgLen:])), nil).Once()

	producer := NewProducer(opener, func() Unmarshallable {
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
