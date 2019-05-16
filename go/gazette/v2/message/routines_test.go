package message

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type RoutinesSuite struct{}

func (s *RoutinesSuite) TestPublishSuccess(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var bk = brokertest.NewBroker(c, etcd, "local", "broker")
	brokertest.CreateJournals(c, bk, brokertest.Journal(pb.JournalSpec{Name: "a/journal"}))

	var rjc = pb.NewRoutedJournalClient(bk.Client(), pb.NoopDispatchRouter{})
	var as = client.NewAppendService(context.Background(), rjc)

	var mapping = func(msg Message) (pb.Journal, Framing, error) {
		return "a/journal", JSONFraming, nil
	}
	var aa, err = Publish(as, mapping, struct{ Data string }{Data: "beer beer beer"})
	c.Check(err, gc.IsNil)
	<-aa.Done()

	// Expect a JSON serialization of the message was written to the mapped journal.
	var r = client.NewReader(context.Background(), rjc, pb.ReadRequest{Journal: "a/journal"})
	b, err := ioutil.ReadAll(r)
	c.Check(string(b), gc.Equals, `{"Data":"beer beer beer"}`+"\n")
	c.Check(err, gc.Equals, client.ErrOffsetNotYetAvailable)

	bk.Tasks.Cancel()
	c.Check(bk.Tasks.Wait(), gc.IsNil)
}

func (s *RoutinesSuite) TestFramingDetermination(c *gc.C) {
	var f, err = FramingByContentType(labels.ContentType_JSONLines)
	c.Check(err, gc.IsNil)
	c.Check(f, gc.Equals, JSONFraming)

	f, err = FramingByContentType(labels.ContentType_ProtoFixed)
	c.Check(err, gc.IsNil)
	c.Check(f, gc.Equals, FixedFraming)

	_, err = FramingByContentType(labels.ContentType_RecoveryLog) // Not a valid message framing.
	c.Check(err, gc.ErrorMatches, `unrecognized `+labels.ContentType+` \(`+labels.ContentType_RecoveryLog+`\)`)
}

func (s *RoutinesSuite) TestLineUnpackingCases(c *gc.C) {
	const bsize = 16
	var buf = bytes.NewBufferString("a line\n" + strings.Repeat("x", bsize*3/2) + "\nextra")
	var br = bufio.NewReaderSize(buf, bsize)

	var p, _ = br.Peek(1)

	// Case 1: line fits in buffer.
	var line, err = UnpackLine(br)
	c.Check(err, gc.IsNil)
	c.Check(&line[0], gc.Equals, &p[0]) // |line| references internal buffer.

	// Case 2: line doesn't fit in buffer.
	line, err = UnpackLine(br)
	c.Check(err, gc.IsNil)
	c.Check(string(line), gc.Equals, strings.Repeat("x", bsize*3/2)+"\n")
	c.Check(&line[0], gc.Not(gc.Equals), &p[0]) // |line| *does not* reference internal buffer.

	// Case 3: EOF without newline and read content is mapped to ErrUnexpectedEOF.
	line, err = UnpackLine(br)
	c.Check(string(line), gc.Equals, "extra")
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// Case 4: EOF without any read content is passed through.
	line, err = UnpackLine(br)
	c.Check(string(line), gc.Equals, "")
	c.Check(err, gc.Equals, io.EOF)
}

func buildPartitionsFuncFixture(count int) PartitionsFunc {
	var parts = &pb.ListResponse{
		Journals: make([]pb.ListResponse_Journal, count),
	}
	for i := range parts.Journals {
		parts.Journals[i].Spec.Name = pb.Journal(fmt.Sprintf("a/topic/part-%03d", i))
		parts.Journals[i].Spec.LabelSet = pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines)
	}
	return func() *pb.ListResponse { return parts }
}

func (s *RoutinesSuite) TestModuloMappingRegressionFixtures(c *gc.C) {
	var parts = buildPartitionsFuncFixture(443) // Prime number.
	var mappingKey = func(msg Message, b []byte) []byte { return append(b, msg.(string)...) }
	var mapping = ModuloMapping(mappingKey, parts)

	// Expect distributed and *stable* routing across partitions.
	for key, expectedPartition := range map[string]int{
		"It always":               119,
		"seems":                   54,
		"impossible":              308,
		"until":                   97,
		"it is done.":             179,
		"- Nelson Mandela":        437,
		"Shall I compare":         84,
		"thee to":                 300,
		"a summer's day?":         214,
		"Thou art more":           135,
		"lovely and more":         118,
		"temperate - Shakespeare": 228,
	} {
		var j, f, err = mapping(key)
		c.Check(f, gc.Equals, JSONFraming)
		c.Check(err, gc.IsNil)
		c.Check(key+"-"+j.String(), gc.Equals,
			fmt.Sprintf("%s-a/topic/part-%03d", key, expectedPartition))
	}
}

func (s *RoutinesSuite) TestRendezvousMappingRegressionFixtures(c *gc.C) {
	var mappingKey = func(msg Message, b []byte) []byte { return append(b, msg.(string)...) }

	var verify = func(mapping MappingFunc) {
		for key, expectedPartition := range map[string]int{
			"It always":               21,
			"seems":                   179,
			"impossible":              50,
			"until":                   13,
			"it is done.":             337,
			"- Nelson Mandela":        29,
			"Shall I compare":         110,
			"thee to":                 395,
			"a summer's day?":         192,
			"Thou art more":           394,
			"lovely and more":         344,
			"temperate - Shakespeare": 75,
		} {
			var j, f, err = mapping(key)
			c.Check(f, gc.Equals, JSONFraming)
			c.Check(err, gc.IsNil)
			c.Check(key+"-"+j.String(), gc.Equals,
				fmt.Sprintf("%s-a/topic/part-%03d", key, expectedPartition))
		}
	}
	// Rendezvous hashing minimizes re-assignments which should occur due to
	// altered partition counts. Expect our fixture happens to be stable despite
	// modest changes to total partitions.
	verify(RendezvousMapping(mappingKey, buildPartitionsFuncFixture(400)))
	verify(RendezvousMapping(mappingKey, buildPartitionsFuncFixture(500)))
}

var _ = gc.Suite(&RoutinesSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
