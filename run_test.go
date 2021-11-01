package parser

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStdoutLineBreaking(t *testing.T) {
	var all []string

	var s = &parserStdout{
		onLines: func(lines []json.RawMessage) error {
			for _, ll := range lines {
				all = append(all, string(ll))
			}
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
	w("one\n")
	// Multiple writes for one line.
	w("two")
	w("three")
	w("four\n")
	// Multiple linebreaks in one write.
	w("five\nsix\nseven\n")

	verify([]string{"one", "twothreefour", "five", "six", "seven"})

	// Worst-case line breaks.
	w("one")
	w("two\nthree\nfour")
	w("five\nsix\nseven")

	verify([]string{"onetwo", "three", "fourfive", "six"})

	w("eight\n")
	w("nine")
	w("\n")
	verify([]string{"seveneight", "nine"})

	// If onLines errors, it calls into onError (which panics in this fixture).
	var err = fmt.Errorf("error!")
	s.onLines = func([]json.RawMessage) error { return err }
	require.PanicsWithValue(t, err, func() { s.Write(nil) })
}
