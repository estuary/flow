package message

import (
	"bufio"
	"encoding/json"
	"fmt"

	"go.gazette.dev/core/labels"
)

type jsonFraming struct{}

type JsonFrameable interface {
	MarshalJsonTo(*bufio.Writer) (int, error)
	UnmarshalJson([]byte) error
}

// ContentType returns labels.ContentType_JSONLines.
func (*jsonFraming) ContentType() string { return labels.ContentType_JSONLines }

// Marshal implements Framing.
func (*jsonFraming) Marshal(msg Frameable, bw *bufio.Writer) error {
	var _, ok = msg.(JsonFrameable)
	if !ok {
		return fmt.Errorf("%#v is not a JsonFrameable", msg)
	}
	if jf, ok := msg.(JsonFrameable); ok {
		_, err := jf.MarshalJsonTo(bw)
		return err
	}
	return json.NewEncoder(bw).Encode(msg)
}

// NewUnmarshalFunc returns an UnmarshalFunc which decodes JSON messages from the Reader.
func (*jsonFraming) NewUnmarshalFunc(r *bufio.Reader) UnmarshalFunc {
	// We cannot use json.NewDecoder, as it buffers internally beyond the
	// precise boundary of a JSON message.
	return func(f Frameable) (err error) {
		var l []byte
		if l, err = UnpackLine(r); err != nil {
			return
		}
		if jf, ok := f.(JsonFrameable); ok {
			return jf.UnmarshalJson(l)
		}
		return json.Unmarshal(l, f)

	}
}

func init() { RegisterFraming(new(jsonFraming)) }
