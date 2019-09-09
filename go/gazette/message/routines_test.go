package message

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/labels"
)

func TestFramingDetermination(t *testing.T) {
	var f, err = FramingByContentType(labels.ContentType_JSONLines)
	assert.NoError(t, err)
	assert.Equal(t, JSONFraming, f)

	f, err = FramingByContentType(labels.ContentType_ProtoFixed)
	assert.NoError(t, err)
	assert.Equal(t, FixedFraming, f)

	_, err = FramingByContentType(labels.ContentType_RecoveryLog) // Not a valid message framing.
	assert.EqualError(t, err, `unrecognized `+labels.ContentType+` (`+labels.ContentType_RecoveryLog+`)`)
}

func TestLineUnpackingCases(t *testing.T) {
	const bsize = 16
	var buf = bytes.NewBufferString("a line\n" + strings.Repeat("x", bsize*3/2) + "\nextra")
	var br = bufio.NewReaderSize(buf, bsize)

	var p, _ = br.Peek(1)

	// Case 1: line fits in buffer.
	var line, err = UnpackLine(br)
	assert.NoError(t, err)
	assert.Equal(t, cap(p), cap(line)) // |line| references internal buffer.

	// Case 2: line doesn't fit in buffer.
	line, err = UnpackLine(br)
	assert.NoError(t, err)
	assert.Equal(t, strings.Repeat("x", bsize*3/2)+"\n", string(line))
	assert.NotEqual(t, cap(p), cap(line)) // |line| *does not* reference internal buffer.

	// Case 3: EOF without newline and read content is mapped to ErrUnexpectedEOF.
	line, err = UnpackLine(br)
	assert.Equal(t, io.ErrUnexpectedEOF, err)
	assert.Equal(t, "extra", string(line))

	// Case 4: EOF without any read content is passed through.
	line, err = UnpackLine(br)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, "", string(line))
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

func TestRandomMapping(t *testing.T) {
	var parts = buildPartitionsFuncFixture(100)
	var mapping = RandomMapping(parts)
	var results = make(map[pb.Journal]struct{})

	for i := 0; i != 101; i++ {
		var j, f, err = mapping(&testMsg{Str: "foobar"})
		assert.NoError(t, err)
		assert.Equal(t, JSONFraming, f)
		results[j] = struct{}{}
	}
	// Probabilistic test; chance of failure is 1e-200.
	assert.True(t, len(results) > 1)
}

func TestModuloMappingRegressionFixtures(t *testing.T) {
	var parts = buildPartitionsFuncFixture(443) // Prime number.
	var mappingKey = func(msg Mappable, w io.Writer) { _, _ = w.Write([]byte(msg.(*testMsg).Str)) }
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
		var j, f, err = mapping(&testMsg{Str: key})
		assert.NoError(t, err)
		assert.Equal(t, JSONFraming, f)
		assert.Equal(t, fmt.Sprintf("%s-a/topic/part-%03d", key, expectedPartition), key+"-"+j.String())
	}
}

func TestRendezvousMappingRegressionFixtures(t *testing.T) {
	var mappingKey = func(msg Mappable, w io.Writer) { _, _ = w.Write([]byte(msg.(*testMsg).Str)) }

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
			var j, f, err = mapping(&testMsg{Str: key})
			assert.NoError(t, err)
			assert.Equal(t, JSONFraming, f)
			assert.Equal(t, fmt.Sprintf("%s-a/topic/part-%03d", key, expectedPartition), key+"-"+j.String())
		}
	}
	// Rendezvous hashing minimizes re-assignments which should occur due to
	// altered partition counts. Expect our fixture happens to be stable despite
	// modest changes to total partitions.
	verify(RendezvousMapping(mappingKey, buildPartitionsFuncFixture(400)))
	verify(RendezvousMapping(mappingKey, buildPartitionsFuncFixture(500)))
}
