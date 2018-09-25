//go:generate protoc -I . -I ../../../vendor -I ../../../../../.. --gogo_out=plugins=grpc:. word_count.proto
package word_count

import (
	"bytes"
	"context"
	"strings"
	"time"
	"unicode"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

type NGram string

func NGramCountMappingKey(m message.Message, b []byte) []byte {
	return append(b, m.(*NGramCount).NGram[:]...)
}

func NewNGramDeltaMapping(ctx context.Context, jc pb.JournalClient) (message.MappingFunc, error) {
	var parts, err = client.NewPolledList(context.Background(), jc, time.Minute,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("topic", "examples/word-count/deltas"),
			},
		})

	if err != nil {
		return nil, err
	}
	return message.ModuloMapping(NGramCountMappingKey, parts.List), nil
}

func NewNGramRelocationsMapping(ctx context.Context, jc pb.JournalClient) (message.MappingFunc, error) {
	var parts, err = client.NewPolledList(context.Background(), jc, time.Minute,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("topic", "examples/word-count/relocations"),
			},
		})

	if err != nil {
		return nil, err
	}
	return message.ModuloMapping(NGramCountMappingKey, parts.List), nil
}

func PublishNGrams(mapping message.MappingFunc, ajc client.AsyncJournalClient, text string, N int) error {
	// Use a very simplistic token strategy: lowercase, letter character sequences.
	var words = strings.FieldsFunc(strings.ToLower(text),
		func(r rune) bool { return !unicode.IsLetter(r) })

	var grams = make([]string, N)
	for i := range grams {
		grams[i] = "START"
	}
	var buf bytes.Buffer
	var record = &NGramCount{Count: 1}

	for i := 0; i != len(words)+N; i++ {
		copy(grams, grams[1:])

		if i < len(words) {
			grams[N-1] = words[i]
		} else {
			grams[N-1] = "END"
		}

		buf.Reset()
		for j, g := range grams {
			buf.WriteString(g)
			if j != N-1 {
				buf.WriteByte(' ')
			}
		}

		record.NGram = NGram(buf.String())
		if _, err := message.Publish(ajc, mapping, record); err != nil {
			return err
		}
	}
	return nil
}
