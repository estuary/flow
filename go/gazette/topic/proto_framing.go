package topic

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FixedFraming is a Framing implementation which encodes messages in a binary
// format with a fixed-length header. Messages must support Size and MarshalTo
// functions for marshal support (eg, generated Protobuf messages satisfy this
// interface). Messages are encoded as a 4-byte magic word for de-synchronization
// detection, followed by a little-endian uint32 length, followed by payload bytes.
var FixedFraming = new(fixedFraming)

const FixedFrameHeaderLength = 8

type fixedFraming struct{}

// Encode implements topic.Framing.
func (*fixedFraming) Encode(msg Message, b []byte) ([]byte, error) {
	var p, ok = msg.(interface {
		Size() int
		MarshalTo([]byte) (int, error)
	})
	if !ok {
		return nil, fmt.Errorf("%+v is not fixed-frameable (must implement Size and MarshalTo)", msg)
	}

	var size = FixedFrameHeaderLength + p.Size()
	var offset = len(b)

	if size > (cap(b) - offset) {
		b = append(b, make([]byte, size)...)
	} else {
		b = b[:offset+size]
	}

	// Header consists of a magic word (for de-sync detection), and a 4-byte length.
	copy(b[offset:offset+4], magicWord[:])
	binary.LittleEndian.PutUint32(b[offset+4:offset+8], uint32(size-FixedFrameHeaderLength))

	if _, err := p.MarshalTo(b[offset+FixedFrameHeaderLength:]); err != nil {
		return nil, err
	}
	return b, nil
}

// Unpack returns the next fixed frame of content from the Reader, including
// the frame header. If the magic word is not detected (indicating a desync),
// Unpack attempts to continue reading until the next magic word, returning
// the interleaved but desynchronized content.
//
// It implements topic.Framing.
func (*fixedFraming) Unpack(r *bufio.Reader) ([]byte, error) {
	var b, err = r.Peek(FixedFrameHeaderLength)

	if err != nil {
		// If buffer just contains a trailing newline, return EOF.
		// This can be the case for hadoop streaming unpacking of PixelEvents.
		if err == io.EOF && len(b) == 1 && b[0] == 0x0a {
			return nil, io.EOF
		}
		if err == io.EOF && len(b) != 0 {
			// If we read at least one byte, then an EOF is unexpected (it should
			// occur only on whole-message boundaries).
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	if !matchesMagicWord(b) {
		// We are not at the expected frame boundary. Scan forward within the buffered
		// region to the beginning of the next magic word. Return the intermediate
		// jumbled frame (this will produce an ErrDesyncDetected on a later Unmarshal).
		b, _ = r.Peek(r.Buffered())

		var i, j = 1, 1 + len(b) - len(magicWord)
		for ; i != j; i++ {
			if matchesMagicWord(b[i:]) {
				break
			}
		}
		r.Discard(i)
		return b[:i], nil
	}

	// Next 4 bytes are encoded size. Combine with header for full frame size.
	var size = FixedFrameHeaderLength + int(binary.LittleEndian.Uint32(b[4:]))

	// Fast path: check if the full frame is available in buffer. Return the
	// buffer internal slice without copying. It is invalidated by the next
	// Unpack (or other Reader operation).
	if b, err = r.Peek(size); err == nil {
		r.Discard(size)
		return b, nil
	}

	// Slow path. Allocate and attempt to Read the full frame.
	b = make([]byte, size)
	_, err = io.ReadFull(r, b)
	return b, err
}

// Unmarshal verifies the frame header and unpacks Message content. If the frame
// header indicates a desync occurred (incorrect magic word), ErrDesyncDetected
// is returned.
//
// It implements topic.Framing.
func (*fixedFraming) Unmarshal(b []byte, msg Message) error {
	var p, ok = msg.(interface {
		Unmarshal([]byte) error
	})

	if !ok {
		return fmt.Errorf("%+v is not fixed-frameable (must implement Unmarshal)", msg)
	} else if !matchesMagicWord(b) {
		return ErrDesyncDetected
	}
	return p.Unmarshal(b[FixedFrameHeaderLength:])
}

func matchesMagicWord(b []byte) bool {
	return b[0] == magicWord[0] && b[1] == magicWord[1] && b[2] == magicWord[2] && b[3] == magicWord[3]
}

var (
	// Error returned by Unmarshal upon detection of an invalid frame.
	ErrDesyncDetected = errors.New("detected de-synchronization")

	magicWord = [4]byte{0x66, 0x33, 0x93, 0x36}
)
