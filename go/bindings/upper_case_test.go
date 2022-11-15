package bindings

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/flow/ops/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// frameableString implements the Frameable interface.
type frameableString string

func (m frameableString) ProtoSize() int { return len(m) }
func (m frameableString) MarshalToSizedBuffer(b []byte) (int, error) {
	copy(b, m)
	return 0, nil
}

func TestLogsForwardedFromService(t *testing.T) {
	var logPublisher = testutil.NewTestLogPublisher(ops.TraceLevel)
	var svc = newUpperCase(logPublisher)

	svc.sendBytes(1, []byte("hello"))
	svc.sendMessage(2, frameableString("world"))
	var _, _, err = svc.poll()
	require.NoError(t, err)

	logPublisher.WaitForLogs(t, time.Millisecond*500, 2)
	logPublisher.RequireEventsMatching(t, []testutil.TestLogEvent{
		{
			Level:   ops.DebugLevel,
			Message: "making stuff uppercase",
			Fields: map[string]interface{}{
				"data_len": 5,
				"sum_len":  5,
			},
		},
		{
			Level:   ops.DebugLevel,
			Message: "making stuff uppercase",
			Fields: map[string]interface{}{
				"data_len": 5,
				"sum_len":  10,
			},
		},
	})

	svc.sendMessage(2, frameableString("whoops"))
	_, _, err = svc.poll()
	// Destroying the service should cause the logging file to be closed, which will result in that
	// last log event. We assert that we git the final log event because it means that destroying
	// the service caused the logging file descriptor to be closed, ending the log forwarding goroutine.
	svc.destroy()

	logPublisher.WaitForLogs(t, time.Millisecond*500, 2)
	logPublisher.RequireEventsMatching(t, []testutil.TestLogEvent{
		{
			Level:   ops.ErrorLevel,
			Message: "whoops",
			Fields: map[string]interface{}{
				"error":     `{"code":2,"message":"whoops"}`,
				"logSource": "uppercase",
			},
		},
		{
			Level:   ops.TraceLevel,
			Message: "finished forwarding logs",
			Fields: map[string]interface{}{
				"jsonLines": 3,
				"textLines": 0,
			},
		},
	})

}

func TestLotsOfLogs(t *testing.T) {
	var logPublisher = testutil.NewTestLogPublisher(ops.DebugLevel)
	var svc = newUpperCase(logPublisher)
	defer svc.destroy()

	var expectedSum = 0
	for _, n := range []int{1, 3, 24, 256, 2048} {
		var expectedLogs []testutil.TestLogEvent
		for i := 0; i < n; i++ {
			expectedSum++
			svc.sendMessage(1, frameableString("f"))
			expectedLogs = append(expectedLogs, testutil.TestLogEvent{
				Level:   ops.DebugLevel,
				Message: "making stuff uppercase",
				Fields: map[string]interface{}{
					"data_len": 1,
					"sum_len":  expectedSum,
				},
			})
		}
		var _, _, err = svc.poll()
		require.NoError(t, err)
		logPublisher.WaitForLogs(t, time.Second, n)
		logPublisher.RequireEventsMatching(t, expectedLogs)
	}
}

func TestUpperServiceFunctional(t *testing.T) {
	var logPublisher = testutil.NewTestLogPublisher(ops.DebugLevel)
	var svc = newUpperCase(logPublisher)
	defer svc.destroy()

	// Test growing |buf|.
	svc.buf = make([]byte, 0, 1)

	svc.sendBytes(1, []byte("hello"))
	svc.sendMessage(2, frameableString("world"))
	var arena, out, err = svc.poll()

	assert.NoError(t, err)
	assert.Len(t, out, 2)
	assert.Equal(t, pf.Arena("HELLOWORLD"), arena)
	assert.Equal(t, []byte("HELLO"), svc.arenaSlice(out[0]))
	assert.Equal(t, []byte("WORLD"), svc.arenaSlice(out[1]))
	assert.Equal(t, 5, int(out[0].code))
	assert.Equal(t, 10, int(out[1].code))

	svc.sendMessage(3, frameableString("bye"))
	arena, out, err = svc.poll()

	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, pf.Arena("BYE"), arena)
	assert.Equal(t, []byte("BYE"), svc.arenaSlice(out[0]))
	assert.Equal(t, 13, int(out[0].code))

	// Trigger an error, and expect it's plumbed through.
	svc.sendBytes(6, []byte("whoops"))
	_, _, err = svc.poll()
	assert.EqualError(t, err, "whoops")
}

func TestNoOpServiceFunctional(t *testing.T) {
	var svc = newNoOpService()
	defer svc.destroy()

	svc.sendBytes(1, []byte("hello"))
	svc.sendBytes(2, []byte("world"))

	var arena, out, err = svc.poll()
	assert.NoError(t, err)
	assert.Len(t, out, 2)
	assert.Empty(t, arena)

	svc.sendBytes(3, []byte("bye"))

	arena, out, err = svc.poll()
	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Empty(t, arena)
}

func TestUpperServiceWithStrides(t *testing.T) {
	var svc = newUpperCase(ops.StdLogger())
	defer svc.destroy()

	for i := 0; i != 4; i++ {
		var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
		var expect = bytes.Repeat([]byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789"), 2)

		svc.sendBytes(1, nil)
		for b := 0; b != len(given); b += 2 {
			svc.sendBytes(2, given[b:b+2])
		}
		svc.sendBytes(3, nil)

		for b := 0; b != len(given); b++ {
			svc.sendBytes(4, given[b:b+1])
		}
		svc.sendBytes(5, nil)

		var got []byte
		var _, out, err = svc.poll()
		assert.NoError(t, err)

		for _, o := range out {
			got = append(got, svc.arenaSlice(o)...)
		}
		assert.Equal(t, expect, got)
		assert.Equal(t, len(given)*2*(i+1), int(out[len(out)-1].code))
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

	_, _, err = svc.invoke(789, []byte("whoops"))
	assert.EqualError(t, err, "Custom { kind: Other, error: \"whoops\" }")
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

	_, _, err = svc.invoke(789, []byte("whoops"))
	assert.EqualError(t, err, "whoops")
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
			var svc = newUpperCase(ops.StdLogger())

			for i := 0; i != b.N; i++ {
				if i%stride == 0 && i > 0 {
					if _, _, err := svc.poll(); err != nil {
						panic(err)
					}
				}
				svc.sendBytes(0, input)
			}
			if _, _, err := svc.poll(); err != nil {
				panic(err)
			}
		})

		b.Run("noop-"+strconv.Itoa(stride), func(b *testing.B) {
			var svc = newNoOpService()

			for i := 0; i != b.N; i++ {
				if i%stride == 0 && i > 0 {
					if _, _, err := svc.poll(); err != nil {
						panic(err)
					}
				}
				svc.sendBytes(0, input)
			}
			if _, _, err := svc.poll(); err != nil {
				panic(err)
			}
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
