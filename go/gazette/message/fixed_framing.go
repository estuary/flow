package message

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.gazette.dev/core/labels"
)

// FixedFrameHeaderLength is the number of leading header bytes of a fixed
// frame, consisting of the word [4]byte{0x66, 0x33, 0x93, 0x36} followed
// by a 4-byte little-endian unsigned length.
const FixedFrameHeaderLength = 8

var (
	// FixedFrameWord is a fixed 4-byte word value which precedes all fixed frame encodings.
	FixedFrameWord = [4]byte{0x66, 0x33, 0x93, 0x36}
	// ErrDesyncDetected is returned by UnpackFixedFrame upon detection of an invalid frame header.
	ErrDesyncDetected = errors.New("detected de-synchronization")
)

// UnpackFixedFrame returns the next fixed frame of content from the Reader,
// including the frame header. If the magic word is not detected (indicating
// a de-sync), UnpackFixedFrame attempts to discard through to the next
// magic word, returning the interleaved but de-synchronized content along
// with ErrDesyncDetected.
func UnpackFixedFrame(r *bufio.Reader) ([]byte, error) {
	var b, err = r.Peek(FixedFrameHeaderLength)

	if err != nil {
		// If we read at least one byte, then an EOF is unexpected (it should
		// occur only on whole-message boundaries). One exception case is
		// a buffer which contains exactly one newline.
		// TODO(johnny): Can we remove newline handling? Helped with Hadoop streaming IIRC.
		if l := len(b); err == io.EOF && l != 0 && (l != 1 || b[0] != 0x0a) {
			err = io.ErrUnexpectedEOF
		}
		if err != io.EOF {
			err = errors.Wrap(err, "Peek(FixedFrameHeaderLength)")
		}
		return nil, err
	}

	if !bytes.Equal(b[:4], FixedFrameWord[:]) {
		// We are not at the expected frame boundary. Scan forward within the buffered
		// region to the beginning of the next magic word. Return the intermediate
		// jumbled frame (this will produce an ErrDesyncDetected on a later Unmarshal).
		b, _ = r.Peek(r.Buffered())

		var i, j = 1, 1 + len(b) - len(FixedFrameWord)
		for ; i != j; i++ {
			if bytes.Equal(b[i:i+4], FixedFrameWord[:]) {
				break
			}
		}
		_, _ = r.Discard(i)
		return b[:i], ErrDesyncDetected
	}

	// Next 4 bytes are encoded size. Combine with header for full frame size.
	var size = FixedFrameHeaderLength + int(binary.LittleEndian.Uint32(b[4:]))

	// Fast path: check if the full frame is available in buffer. Return the
	// buffer internal slice without copying. It is invalidated by the next
	// UnpackFixedFrame (or other Reader operation).
	if b, err = r.Peek(size); err == nil {
		_, _ = r.Discard(size)
		return b, nil
	}

	// Slow path: allocate and attempt to read the full frame.
	b = make([]byte, size)
	if _, err = io.ReadFull(r, b); err == nil {
		return b, nil
	} else if err == io.EOF {
		err = io.ErrUnexpectedEOF // Always unexpected (having read a header).
	}
	return b, errors.Wrapf(err, "reading frame (size %d)", size)
}

// ProtoFrameable is the Frameable interface required by a Framing of protobuf messages.
type ProtoFrameable interface {
	ProtoSize() int
	MarshalTo([]byte) (int, error)
	Unmarshal([]byte) error
}

// Encode a ProtoFrameable by appending a fixed frame into the []byte buffer,
// which will be grown if needed and returned.
func EncodeFixedProtoFrame(p ProtoFrameable, b []byte) ([]byte, error) {
	var size = FixedFrameHeaderLength + p.ProtoSize()
	var offset = len(b)

	if size > (cap(b) - offset) {
		b = append(b, make([]byte, size)...)
	} else {
		b = b[:offset+size]
	}

	// Header consists of a magic word (for de-sync detection), and a 4-byte length.
	copy(b[offset:offset+4], FixedFrameWord[:])
	binary.LittleEndian.PutUint32(b[offset+4:offset+8], uint32(size-FixedFrameHeaderLength))

	if _, err := p.MarshalTo(b[offset+FixedFrameHeaderLength:]); err != nil {
		return nil, err
	}
	return b, nil
}

type protoFixedFraming struct{}

func (protoFixedFraming) ContentType() string { return labels.ContentType_ProtoFixed }

func (protoFixedFraming) Marshal(msg Frameable, bw *bufio.Writer) error {
	var pf, ok = msg.(ProtoFrameable)
	if !ok {
		return fmt.Errorf("%#v is not a ProtoFramable", msg)
	}
	var b, err = EncodeFixedProtoFrame(pf, bufferPool.Get().([]byte))
	if err == nil {
		_, _ = bw.Write(b)
	}
	bufferPool.Put(b[:0])
	return err
}

func (protoFixedFraming) NewUnmarshalFunc(r *bufio.Reader) UnmarshalFunc {
	return func(f Frameable) error {
		var ff, ok = f.(ProtoFrameable)
		if !ok {
			return fmt.Errorf("%#v is not a ProtoFramable", f)
		} else if b, err := UnpackFixedFrame(r); err != nil {
			return err
		} else {
			return ff.Unmarshal(b[FixedFrameHeaderLength:])
		}
	}
}

// bufferPool pools buffers used for MarshalTo encodings.
var bufferPool = sync.Pool{New: func() interface{} { return make([]byte, 0, 1024) }}

func init() { RegisterFraming(new(protoFixedFraming)) }
