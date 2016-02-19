package message

import (
	"io"

	"github.com/pippio/gazette/topic"
)

// Decoder decodes sequential framed messages from a journal.
type Decoder struct {
	reader io.Reader
	buffer []byte
}

func NewDecoder(reader io.Reader) Decoder {
	return Decoder{
		reader: reader,
	}
}

// Decode reads the next message from its input and unmarshals it into |msg|.
// Reads are performed without extra buffering, and it is safe to interleave
// calls to Decode() with other reads.
func (d *Decoder) Decode(msg topic.Unmarshallable) error {
	_, err := Parse(msg, d.reader, &d.buffer)
	return err
}
