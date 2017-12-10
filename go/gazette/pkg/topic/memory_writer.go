package topic

import (
	"bufio"
	"bytes"
	"io"

	"github.com/LiveRamp/gazette/pkg/journal"
)

// MemoryWriter is an implementation of journal.Writer which uses a provided Framing and
// message initializer to decode and capture messages as they are written. The intended
// use is within unit tests which publish and subsequently verify expected messages.
type MemoryWriter struct {
	framing Framing
	new     func() Message

	Messages []Envelope
}

func NewMemoryWriter(framing Framing, new func() Message) *MemoryWriter {
	return &MemoryWriter{framing: framing, new: new}
}

func (w *MemoryWriter) Write(j journal.Name, b []byte) (*journal.AsyncAppend, error) {
	return w.ReadFrom(j, bytes.NewReader(b))
}

func (w *MemoryWriter) ReadFrom(j journal.Name, r io.Reader) (*journal.AsyncAppend, error) {
	var br = bufio.NewReader(r)

	var result = &journal.AsyncAppend{
		Ready: make(chan struct{}),
	}
	close(result.Ready)

	for {
		var frame, err = w.framing.Unpack(br)

		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return result, err
		}
		var msg = w.new()

		if err = w.framing.Unmarshal(frame, msg); err != nil {
			return result, err
		}

		w.Messages = append(w.Messages, Envelope{
			Mark:    journal.Mark{Journal: j},
			Message: msg,
		})
	}
}
