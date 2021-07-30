package airbyte

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStdoutRecordBreaking(t *testing.T) {
	var all []string

	var s = &connectorStdout{
		onNew: func() interface{} { return new(string) },
		onDecode: func(i interface{}) error {
			all = append(all, *i.(*string))
			return nil
		},
		onError: func(err error) { panic(err) },
	}

	var w = func(p string) {
		var n, err = s.Write([]byte(p))
		require.Equal(t, len(p), n)
		require.NoError(t, err)
	}

	var verify = func(v []string) {
		require.Equal(t, v, all)
		all = nil
	}

	// Single line.
	w("\"one\"\n")
	// Multiple writes for one line.
	w("\"two")
	w("three")
	w("four\"\n")
	// Multiple linebreaks in one write.
	w("\"five\"\n\"six\"\n\"seven\"\n")

	verify([]string{"one", "twothreefour", "five", "six", "seven"})

	// Worst-case line breaks.
	w("\"one")
	w("two\"\n\"three\"\n\"four")
	w("five\"\n\"six\"\n\"seven")

	verify([]string{"onetwo", "three", "fourfive", "six"})

	w("eight\"\n\"")
	w("nine\"")
	w("\n")
	verify([]string{"seveneight", "nine"})

	// A Close on a newline is okay.
	require.NoError(t, s.Close())
	// But a Close with partial data panics.
	w("\"extra")
	require.PanicsWithErrorf(t, "connector stdout closed without a final newline: \"\\\"extra\"", func() { s.Close() }, "")

	// If onDecode errors, it calls into onError (which panics in this fixture).
	var err = fmt.Errorf("error!")
	s.onDecode = func(i interface{}) error { return err }
	require.PanicsWithValue(t, err, func() { s.Write([]byte("\"whoops\"\n")) })
}

func TestStderrCapture(t *testing.T) {
	var s = new(connectorStderr)

	var n, err = s.Write([]byte("whoops"))
	require.Equal(t, 6, n)
	require.NoError(t, err)
	require.Equal(t, "whoops", s.err.String())

	// Expect it caps the amount of output collected.
	s.Write(bytes.Repeat([]byte("x"), maxStderrBytes))
	require.Equal(t, maxStderrBytes, s.err.Len())
}
