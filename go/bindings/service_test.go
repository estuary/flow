package bindings

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// frameableString implements the Frameable interface.
type frameableString string

func (m frameableString) ProtoSize() int { return len(m) }
func (m frameableString) MarshalToSizedBuffer(b []byte) (int, error) {
	copy(b, m)
	return 0, nil
}

func TestUpperServiceFunctional(t *testing.T) {
	var svc = newUpperCase()

	// Cover frameBuf growing.
	svc.frameBuf = make([]byte, 0, 1)

	svc.SendBytes(1, []byte("hello"))
	svc.SendMessage(2, frameableString("world"))
	var arena, responses, err = svc.Poll()

	assert.Equal(t, []byte("HELLOWORLD"), arena)
	assert.Equal(t, []Frame{
		{Data: []byte("HELLO"), Code: 5},
		{Data: []byte("WORLD"), Code: 10},
	}, responses)
	assert.NoError(t, err)

	svc.SendMessage(3, frameableString("bye"))
	arena, responses, err = svc.Poll()

	assert.Equal(t, []byte("BYE"), arena)
	assert.Equal(t, []Frame{
		{Data: []byte("BYE"), Code: 13},
	}, responses)
	assert.NoError(t, err)

	// Trigger an error, and expect it's plumbed through.
	svc.SendBytes(6, []byte("whoops!"))
	_, _, err = svc.Poll()
	assert.EqualError(t, err, "Custom { kind: Other, error: \"whoops!\" }")
}

func TestNoOpServiceFunctional(t *testing.T) {
	var svc = newNoOpService()

	svc.SendBytes(1, []byte("hello"))
	svc.SendBytes(2, []byte("world"))

	var arena, responses, err = svc.Poll()
	assert.Empty(t, arena)
	assert.Equal(t, []Frame{{}, {}}, responses)
	assert.NoError(t, err)

	svc.SendBytes(3, []byte("bye"))

	arena, responses, err = svc.Poll()
	assert.Empty(t, arena)
	assert.Equal(t, []Frame{{}}, responses)
	assert.NoError(t, err)
}

func TestUpperServiceWithStrides(t *testing.T) {
	var svc = newUpperCase()

	for i := 0; i != 4; i++ {
		var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
		var expect = bytes.Repeat([]byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789"), 2)

		svc.SendBytes(1, nil)
		for b := 0; b != len(given); b += 2 {
			svc.SendBytes(2, given[b:b+2])
		}
		svc.SendBytes(3, nil)

		for b := 0; b != len(given); b += 1 {
			svc.SendBytes(4, given[b:b+1])
		}
		svc.SendBytes(5, nil)

		var got []byte
		var _, responses, err = svc.Poll()
		assert.NoError(t, err)

		for _, r := range responses {
			got = append(got, r.Data...)
		}
		assert.Equal(t, expect, got)
		assert.Equal(t, len(given)*2*(i+1), int(responses[len(responses)-1].Code))
	}
}

func TestUpperServiceNaive(t *testing.T) {
	var svc = newUpperCaseNaive()

	var code, data, err = svc.invoke(123, []byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, int(code))
	assert.Equal(t, data, []byte("HELLO"))

	var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
	var expect = []byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789")

	code, data, err = svc.invoke(456, given)
	assert.NoError(t, err)
	assert.Equal(t, 5+len(given), int(code))
	assert.Equal(t, expect, data)

	_, _, err = svc.invoke(789, []byte("whoops!"))
	assert.EqualError(t, err, "Custom { kind: Other, error: \"whoops!\" }")
}

func TestUpperServiceGo(t *testing.T) {
	var svc = newUpperCaseGo()

	var code, data, err = svc.invoke(123, []byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, int(code))
	assert.Equal(t, data, []byte("HELLO"))

	var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
	var expect = []byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789")

	code, data, err = svc.invoke(456, given)
	assert.NoError(t, err)
	assert.Equal(t, 5+len(given), int(code))
	assert.Equal(t, expect, data)

	_, _, err = svc.invoke(789, []byte("whoops!"))
	assert.EqualError(t, err, "whoops!")
}

func BenchmarkUpperService(b *testing.B) {
	var strides = []int{
		1,  // Worst case.
		3,  // Almost worst case: 3 separate invocations.
		4,  // Single 4-stride invocation.
		11, // 4 + 4 + 1 + 1 + 1
		15, // 4 + 4 + 4 + 1 + 1 + 1
		17, // 16 + 1
		31, // 16 + 4 + 4 + 4 + 1 + 1 + 1
		32, // 16 + 16
		63, // 16 + 16 + 16 + 4 + 4 + 4 + 1 + 1 + 1
		137,
		426,
	}
	var input = []byte("hello world")

	for _, stride := range strides {
		b.Run("cgo-"+strconv.Itoa(stride), func(b *testing.B) {
			var svc = newUpperCase()

			for i := 0; i != b.N; i++ {
				if i%stride == 0 && i > 0 {
					svc.Poll()
				}
				svc.SendBytes(0, input)
			}
			var _, _, _ = svc.Poll()
		})

		b.Run("noop-"+strconv.Itoa(stride), func(b *testing.B) {
			var svc = newNoOpService()

			for i := 0; i != b.N; i++ {
				if i%stride == 0 && i > 0 {
					svc.Poll()
				}
				svc.SendBytes(0, input)
			}
			var _, _, _ = svc.Poll()
		})
	}
}

func BenchmarkUpperServiceNaive(b *testing.B) {
	var svc = newUpperCaseNaive()
	var input = []byte("hello world")

	for i := 0; i != b.N; i++ {
		_, _, _ = svc.invoke(123, input)
	}
}

func BenchmarkUpperServiceGo(b *testing.B) {
	var svc = newUpperCaseGo()
	var input = []byte("hello world")

	for i := 0; i != b.N; i++ {
		_, _, _ = svc.invoke(123, input)
	}
}
