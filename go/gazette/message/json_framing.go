package message

import (
	"bufio"
	"encoding/json"

	"go.gazette.dev/core/labels"
)

type jsonFraming struct{}

// ContentType returns labels.ContentType_JSONLines.
func (*jsonFraming) ContentType() string { return labels.ContentType_JSONLines }

// Marshal implements Framing.
func (*jsonFraming) Marshal(msg Frameable, bw *bufio.Writer) error {
	return json.NewEncoder(bw).Encode(msg)
}

// NewUnmarshalFunc returns an UnmarshalFunc which decodes JSON messages from the Reader.
func (*jsonFraming) NewUnmarshalFunc(r *bufio.Reader) UnmarshalFunc {
	// We cannot use json.NewDecoder, as it buffers internally beyond the
	// precise boundary of a JSON message.
	return func(f Frameable) error {
		if l, err := UnpackLine(r); err != nil {
			return err
		} else {
			return json.Unmarshal(l, f)
		}
	}
}

func init() { RegisterFraming(new(jsonFraming)) }
