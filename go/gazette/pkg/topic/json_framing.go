package topic

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
)

// JsonFraming is a Framing implementation which encodes messages as line-
// delimited JSON. Messages must be encode-able by the encoding/json package.
var JsonFraming = new(jsonFraming)

type jsonFraming struct{}

// Encode implements topic.Framing.
func (*jsonFraming) Encode(msg Message, b []byte) ([]byte, error) {
	var buf = bytes.NewBuffer(b)

	if err := json.NewEncoder(buf).Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unpack implements topic.Framing.
func (*jsonFraming) Unpack(r *bufio.Reader) ([]byte, error) {
	return UnpackLine(r)
}

// Unmarshal implements topic.Framing.
func (*jsonFraming) Unmarshal(line []byte, msg Message) error {
	return json.Unmarshal(line, msg)
}

// UnpackLine returns bytes through to the first encountered newline "\n". If
// the complete line is in the Reader buffer, no alloc or copy is needed.
func UnpackLine(r *bufio.Reader) ([]byte, error) {
	// Fast path: a line is fully contained in the buffer.
	var line, err = r.ReadSlice('\n')

	if err == bufio.ErrBufferFull {
		// Slow path: the line spills across multiple buffer fills.
		err = nil

		line = append([]byte(nil), line...) // Copy as |line| references an internal buffer.
		var rest []byte

		if rest, err = r.ReadBytes('\n'); err == nil {
			line = append(line, rest...)
		}
	}

	if err == io.EOF && len(line) != 0 {
		// If we read at least one byte, then an EOF is unexpected (it should
		// occur only on whole-message boundaries).
		err = io.ErrUnexpectedEOF
	}
	return line, err
}
