package topic

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
)

const kHeaderLength = 8

var (
	ErrInsufficientBuffer = errors.New("insufficient buffer")
	ErrDesyncDetected     = errors.New("detected de-synchronization")
)

func frame(desc *TopicDescription, msg interface{}, buf *[]byte) error {
	size := desc.Size(msg) + kHeaderLength
	if size > cap(*buf) {
		*buf = sizeBuffer(*buf, size)
	}
	*buf = (*buf)[:size]
	out := *buf

	// Header is a magic word (for de-sync detection), and a 4-byte length.
	out[0] = 0x66
	out[1] = 0x33
	out[2] = 0x93
	out[3] = 0x36
	binary.LittleEndian.PutUint32(out[4:], uint32(size-kHeaderLength))

	if err := desc.MarshalTo(msg, out[kHeaderLength:size]); err != nil {
		*buf = (*buf)[:0] // Truncate.
		return err
	}
	return nil
}

func parse(r io.Reader, desc *TopicDescription, buf *[]byte,
) (delta int, msg interface{}, err error) {
	var expectWord = [4]byte{0x66, 0x33, 0x93, 0x36}
	var header [kHeaderLength]byte

	// Scan until the magic word is read.
	if n, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	} else {
		delta += n
	}
	var desyncErr error
	for bytes.Compare(header[:4], expectWord[:]) != 0 {
		if delta == kHeaderLength {
			// Though we can recover, record that a desync happened.
			desyncErr = ErrDesyncDetected
		}
		// Shift and read one byte.
		copy(header[:], header[1:])
		if n, err := r.Read(header[7:]); err == io.EOF {
			return 0, nil, io.ErrUnexpectedEOF
		} else if err != nil {
			return 0, nil, err
		} else {
			delta += n
		}
	}
	// Next 4 bytes are message length.
	length := int(binary.LittleEndian.Uint32(header[4:]))
	*buf = (*buf)[:0] // Truncate.
	*buf = sizeBuffer(*buf, length)
	tmp := (*buf)[:length]

	if n, err := io.ReadFull(r, tmp); err == io.EOF {
		return 0, nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, nil, err
	} else {
		delta += n
	}

	if msg, err = desc.Unmarshal(tmp); err != nil {
		return delta, nil, err
	} else {
		return delta, msg, desyncErr
	}
}

func sizeBuffer(buf []byte, size int) []byte {
	if avail := cap(buf) - len(buf); size > avail {
		var alloc []byte

		if cap(buf)+avail > size {
			alloc = make([]byte, len(buf), cap(buf)*2)
		} else {
			alloc = make([]byte, len(buf), cap(buf)+size-avail)
		}
		copy(alloc[:], buf[:])
		return alloc
	}
	return buf
}

type MessageScanner struct {
	topic  *TopicDescription
	reader io.ReadCloser
	buffer []byte

	nextOffset *int64
	Message    interface{}
}

func NewMessageScanner(topic *TopicDescription, reader io.ReadCloser,
	offset *int64) *MessageScanner {
	return &MessageScanner{topic: topic, reader: reader, nextOffset: offset}
}

func (s *MessageScanner) Next() (err error, recoverable bool) {
	if delta, msg, err := parse(s.reader, s.topic, &s.buffer); delta == 0 {
		return err, false
	} else {
		*s.nextOffset += int64(delta)
		s.Message = msg
		return err, true
	}
}

func (s *MessageScanner) Close() error {
	return s.reader.Close()
}

// Decodes a single message of |topic| from |frame|. This is primarily
// a test-support method.
func ParseFromFrame(topic *TopicDescription, frame []byte) (interface{}, error) {
	var offset int64
	s := NewMessageScanner(topic, ioutil.NopCloser(bytes.NewReader(frame)), &offset)

	if err, _ := s.Next(); err != nil {
		return nil, err
	}
	return s.Message, nil
}
