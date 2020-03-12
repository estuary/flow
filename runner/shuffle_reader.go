package runner

import (
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/message"
)

// Foo is a bar.
func Foo(s consumer.Shard, journal pb.Journal, offset pb.Offset) message.Iterator {

	return nil
}
