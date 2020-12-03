package bindings

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpperService(t *testing.T) {
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
		var _, responses = svc.Poll()
		for _, r := range responses {
			got = append(got, r.Data...)
		}
		assert.Equal(t, expect, got)
		assert.Equal(t, len(given)*2*(i+1), int(responses[len(responses)-1].Code))
	}
}

func TestUpperServiceNaieve(t *testing.T) {
	var code, data = upperCaseNaieve(123, []byte("hello"))
	assert.Equal(t, 5, int(code))
	assert.Equal(t, data, []byte("HELLO"))

	var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
	var expect = []byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789")

	code, data = upperCaseNaieve(123, given)
	assert.Equal(t, len(given), int(code))
	assert.Equal(t, expect, data)
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
		b.Run(strconv.Itoa(stride), func(b *testing.B) {
			var svc = newUpperCase()

			for i := 0; i != b.N; i++ {
				if i%stride == 0 && i > 0 {
					svc.Poll()
				}
				svc.SendBytes(0, input)
			}
			var _, _ = svc.Poll()
		})
	}
}

func BenchmarkUpperServiceNaieve(b *testing.B) {
	var input = []byte("hello world")

	for i := 0; i != b.N; i++ {
		_, _ = upperCaseNaieve(123, input)
	}
}
