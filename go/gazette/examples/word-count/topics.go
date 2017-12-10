package word_count

import (
	"bufio"

	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/topic"
)

type Sentence struct {
	Str string
}

type Record struct {
	Word  string
	Count int
}

var (
	Sentences = &topic.Description{
		Name: "examples/word-count/sentences",
	}
	Deltas = &topic.Description{
		Name: "examples/word-count/deltas",
	}
	Counts = &topic.Description{
		Name: "examples/word-count/counts",
	}
)

func init() {
	Sentences.Framing = sentenceFraming{}
	Sentences.GetMessage = func() topic.Message { return new(Sentence) }
	Sentences.PutMessage = func(m topic.Message) {}
	Sentences.Partitions = func() []journal.Name { return []journal.Name{journal.Name(Sentences.Name)} }
	Sentences.MappedPartition = func(topic.Message) journal.Name { return journal.Name(Sentences.Name) }

	Deltas.Framing = topic.JsonFraming
	Deltas.GetMessage = func() topic.Message { return new(Record) }
	Deltas.PutMessage = func(m topic.Message) {}
	Deltas.Partitions = topic.EnumeratePartitions(Deltas.Name, 8)
	Deltas.MappedPartition = topic.ModuloPartitionMapping(Deltas.Partitions,
		func(m topic.Message, b []byte) []byte {
			return append(b, m.(*Record).Word...)
		})

	Counts.Framing = topic.JsonFraming
	Counts.GetMessage = func() topic.Message { return new(Record) }
	Counts.PutMessage = func(m topic.Message) {}
	Counts.Partitions = func() []journal.Name { return []journal.Name{journal.Name(Counts.Name)} }
	Counts.MappedPartition = func(topic.Message) journal.Name { return journal.Name(Counts.Name) }
}

type sentenceFraming struct{}

// Encode implements topic.Framing.
func (sentenceFraming) Encode(msg topic.Message, b []byte) ([]byte, error) {
	var s = msg.(*Sentence)
	return append(b[:0], s.Str...), nil
}

// Unpack implements topic.Framing.
func (sentenceFraming) Unpack(r *bufio.Reader) ([]byte, error) {
	return topic.UnpackLine(r)
}

// Unmarshal implements topic.Framing.
func (sentenceFraming) Unmarshal(line []byte, msg topic.Message) error {
	msg.(*Sentence).Str = string(line)
	return nil
}
