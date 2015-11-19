package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

const HeaderLength = 8

var ErrDesyncDetected = errors.New("detected de-synchronization")

// Frames |m| into |buf|. If |buf| doesn't have enough capacity, it is re-allocatted.
func Frame(m Marshallable, buf *[]byte) error {
	length := m.Size() + HeaderLength
	sizeBuffer(buf, length)
	out := *buf

	// Header is a magic word (for de-sync detection), and a 4-byte length.
	out[0] = 0x66
	out[1] = 0x33
	out[2] = 0x93
	out[3] = 0x36
	binary.LittleEndian.PutUint32(out[4:], uint32(length-HeaderLength))

	if _, err := m.MarshalTo(out[HeaderLength:length]); err != nil {
		*buf = (*buf)[:0] // Truncate.
		return err
	}
	return nil
}

// Parses the next framed message from |r| into |m|, using |buf|. If |buf|
// doesn't have enough capacity, it is re-allocated. Returned |delta| is
// non-zero iff a message was successfully parsed. If |err| is
// ErrDesyncDetected, a message was still parsed and a subsequent read
// may succeed.
func Parse(m Unmarshallable, r io.Reader, buf *[]byte) (delta int, err error) {
	var expectWord = [4]byte{0x66, 0x33, 0x93, 0x36}
	var header [HeaderLength]byte

	// Scan until the magic word is read.
	if n, err := io.ReadFull(r, header[:]); err != nil {
		return 0, err
	} else {
		delta += n
	}
	var desyncErr error
	for bytes.Compare(header[:4], expectWord[:]) != 0 {
		if delta == HeaderLength {
			// Though we can recover, record that a desync happened.
			desyncErr = ErrDesyncDetected
		}
		// Shift and read one byte.
		copy(header[:], header[1:])
		if n, err := r.Read(header[7:]); err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		} else if err != nil {
			return 0, err
		} else {
			delta += n
		}
	}
	// Next 4 bytes are message length.
	length := int(binary.LittleEndian.Uint32(header[4:]))
	sizeBuffer(buf, length)

	if n, err := io.ReadFull(r, *buf); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	} else {
		delta += n
	}

	if err = m.Unmarshal(*buf); err != nil {
		return delta, err
	}

	if fix, ok := m.(Fixupable); ok {
		if err = fix.Fixup(); err != nil {
			return delta, err
		}
	}
	return delta, desyncErr
}

// Parses |m| from |frame|, which is expected to be an exactly-sized frame.
func ParseExactFrame(m Unmarshallable, frame []byte) error {
	var buf []byte
	var reader = bytes.NewReader(frame)

	_, err := Parse(m, reader, &buf)
	if err != nil {
		return err
	} else if reader.Len() != 0 {
		return errors.New("extra frame content")
	}
	return nil
}

// Size |buf| to |length|. If |buf| has insufficient capacity, it is re-allocated.
func sizeBuffer(buf *[]byte, length int) {
	if cap(*buf) < length {
		*buf = make([]byte, length, cap(*buf)+length)
	} else {
		*buf = (*buf)[:length]
	}
}
