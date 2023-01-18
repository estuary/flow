package connector

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestDecryptCases(t *testing.T) {

	var doCase = func(t *testing.T) {
		var path = filepath.Join("testdata", filepath.Base(t.Name())) + ".json"

		var data, err = ioutil.ReadFile(path)
		require.NoError(t, err)

		decrypted, err := DecryptConfig(context.Background(), data)
		require.NoError(t, err)

		pretty, err := json.MarshalIndent(decrypted, "", "  ")
		require.NoError(t, err)

		cupaloy.SnapshotT(t, string(pretty))
	}

	t.Run("not-encrypted", doCase)
	t.Run("no-suffix", doCase)
	t.Run("under-suffix", doCase)
	t.Run("hyphen-suffix", doCase)
	t.Run("empty-input", doCase)
}
