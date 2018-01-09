// shuffler is a consumer plugin (eg, should be built with
// `go build --buildmode=plugin`).
package main

import (
	"strings"
	"unicode"

	"github.com/LiveRamp/gazette/examples/word-count"
	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/topic"
)

type shuffler struct{}

func (shuffler) Topics() []*topic.Description {
	return []*topic.Description{word_count.Sentences}
}

func (shuffler) Consume(env topic.Envelope, s consumer.Shard, pub *topic.Publisher) error {
	var words = strings.FieldsFunc(env.Message.(*word_count.Sentence).Str,
		func(r rune) bool { return !unicode.IsLetter(r) })

	for _, w := range words {
		if _, err := pub.Publish(&word_count.Record{Word: w, Count: 1}, word_count.Deltas); err != nil {
			return err
		}
	}
	return nil
}

func (shuffler) Flush(s consumer.Shard, pub *topic.Publisher) error { return nil }

func main() {} // Not called.
var Consumer consumer.Consumer = shuffler{}
