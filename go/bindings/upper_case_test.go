package bindings

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
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
	var logs = make(chan ops.Log, 1)
	var publisher = newChanPublisher(logs, pf.LogLevel_trace)

	var svc = newUpperCase(publisher)
	svc.sendBytes(1, []byte("hello"))
	svc.sendMessage(2, frameableString("world"))
	var _, _, err = svc.poll()
	require.NoError(t, err)

	var actual = <-logs
	require.Equal(t, actual.Level, pf.LogLevel_debug)
	require.Equal(t, actual.Message, "making stuff uppercase")
	require.Equal(t, string(actual.Fields), `{"data_len":5,"module":"bindings::upper_case","sum_len":5}`)

	actual = <-logs
	require.Equal(t, string(actual.Fields), `{"data_len":5,"module":"bindings::upper_case","sum_len":10}`)

	svc.sendMessage(2, frameableString("whoops"))
	_, _, err = svc.poll()

	require.EqualError(t, err, "whoops")

	actual = <-logs
	require.Equal(t, actual.Level, pf.LogLevel_error)
	require.Equal(t, actual.Message, "whoops")
	require.Equal(t, string(actual.Fields), `{"error":"whoops","module":"bindings::service"}`)

	// Destroying the service should cause the logging file to be closed, which will result in this
	// last log event. We assert that we get the final log event because it means that destroying
	// the service caused the logging file descriptor to be closed, ending the log forwarding goroutine.
	svc.destroy()

	actual = <-logs
	require.Equal(t, actual.Level, pf.LogLevel_trace)
	require.Equal(t, actual.Message, "dropped service")
	require.Equal(t, string(actual.Fields), `{"module":"bindings::service"}`)
}

func TestLotsOfLogs(t *testing.T) {
	var logs = make(chan ops.Log, 2048)
	var publisher = newChanPublisher(logs, pf.LogLevel_trace)
	var svc = newUpperCase(publisher)

	var expectedSum = 0
	for _, n := range []int{1, 3, 24, 256, 2048} {
		for i := 0; i != n; i++ {
			svc.sendMessage(1, frameableString("f"))
		}

		var _, _, err = svc.poll()
		require.NoError(t, err)

		for i := 0; i != n; i++ {
			expectedSum++
			var actual = <-logs
			require.Equal(t, string(actual.Fields),
				fmt.Sprintf(`{"data_len":1,"module":"bindings::upper_case","sum_len":%d}`, expectedSum))
		}
	}

	svc.destroy()
	var actual = <-logs
	require.Equal(t, actual.Message, "dropped service")
}

func TestUpperServiceFunctional(t *testing.T) {
	var svc = newUpperCase(localPublisher)
	defer svc.destroy()

	// Test growing |buf|.
	svc.buf = make([]byte, 0, 1)

	svc.sendBytes(1, []byte("hello"))
	svc.sendMessage(2, frameableString("world"))
	var arena, out, err = svc.poll()

	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, pf.Arena("HELLOWORLD"), arena)
	require.Equal(t, []byte("HELLO"), svc.arenaSlice(out[0]))
	require.Equal(t, []byte("WORLD"), svc.arenaSlice(out[1]))
	require.Equal(t, 5, int(out[0].code))
	require.Equal(t, 10, int(out[1].code))

	svc.sendMessage(3, frameableString("bye"))
	arena, out, err = svc.poll()

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, pf.Arena("BYE"), arena)
	require.Equal(t, []byte("BYE"), svc.arenaSlice(out[0]))
	require.Equal(t, 13, int(out[0].code))

	// Trigger an error, and expect it's plumbed through.
	svc.sendBytes(6, []byte("whoops"))
	_, _, err = svc.poll()
	require.EqualError(t, err, "whoops")
}

func TestNoOpServiceFunctional(t *testing.T) {
	var svc = newNoOpService(localPublisher)
	defer svc.destroy()

	svc.sendBytes(1, []byte("hello"))
	svc.sendBytes(2, []byte("world"))

	var arena, out, err = svc.poll()
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Empty(t, arena)

	svc.sendBytes(3, []byte("bye"))

	arena, out, err = svc.poll()
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Empty(t, arena)
}

func TestUpperServiceWithStrides(t *testing.T) {
	var svc = newUpperCase(localPublisher)
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
		require.NoError(t, err)

		for _, o := range out {
			got = append(got, svc.arenaSlice(o)...)
		}
		require.Equal(t, expect, got)
		require.Equal(t, len(given)*2*(i+1), int(out[len(out)-1].code))
	}
}

func TestUpperServiceNaive(t *testing.T) {
	var svc = newUpperCaseNaive()

	var code, data, err = svc.invoke(123, []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, int(code))
	require.Equal(t, data, []byte("HELLO"))

	var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
	var expect = []byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789")

	code, data, err = svc.invoke(456, given)
	require.NoError(t, err)
	require.Equal(t, 5+len(given), int(code))
	require.Equal(t, expect, data)

	_, _, err = svc.invoke(789, []byte("whoops"))
	require.EqualError(t, err, "Custom { kind: Other, error: \"whoops\" }")
}

func TestUpperServiceGo(t *testing.T) {
	var svc = newUpperCaseGo()

	var code, data, err = svc.invoke(123, []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, int(code))
	require.Equal(t, data, []byte("HELLO"))

	var given = []byte("abcd0123efghijklm456nopqrstuvwxyz789")
	var expect = []byte("ABCD0123EFGHIJKLM456NOPQRSTUVWXYZ789")

	code, data, err = svc.invoke(456, given)
	require.NoError(t, err)
	require.Equal(t, 5+len(given), int(code))
	require.Equal(t, expect, data)

	_, _, err = svc.invoke(789, []byte("whoops"))
	require.EqualError(t, err, "whoops")
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
			var svc = newUpperCase(localPublisher)

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
			var svc = newNoOpService(localPublisher)

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

var localPublisher = ops.NewLocalPublisher(
	labels.ShardLabeling{
		Build:    "the-build",
		LogLevel: pf.LogLevel_debug,
		Range: pf.RangeSpec{
			KeyBegin:    0x00001111,
			KeyEnd:      0x11110000,
			RClockBegin: 0x00002222,
			RClockEnd:   0x22220000,
		},
		TaskName: "some-tenant/task/name",
		TaskType: labels.TaskTypeCapture,
	},
)

// chanPublisher sends Log instances to a wrapped channel.
type chanPublisher struct {
	logs   chan<- ops.Log
	labels labels.ShardLabeling
}

var _ ops.Publisher = &chanPublisher{}

func newChanPublisher(ch chan<- ops.Log, level pf.LogLevel) *chanPublisher {
	var labels = localPublisher.Labels()
	labels.LogLevel = level

	return &chanPublisher{
		logs:   ch,
		labels: labels,
	}
}

func (c *chanPublisher) PublishLog(log ops.Log)       { c.logs <- log }
func (c *chanPublisher) Labels() labels.ShardLabeling { return c.labels }
