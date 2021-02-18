package bindings

import (
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestBuildCatalog(t *testing.T) {
	var tmpdir, err = ioutil.TempDir("", "build-catalog")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	var transport = &http.Transport{}
	transport.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	var client = &http.Client{Transport: transport}

	hadUserErrors, err := BuildCatalog(pf.BuildAPI_Config{
		Directory:         tmpdir,
		Source:            "../../examples/flow.yaml",
		CatalogPath:       filepath.Join(tmpdir, "catalog.db"),
		TypescriptCompile: false,
		TypescriptPackage: false,
	}, client)
	require.NoError(t, err)

	require.False(t, hadUserErrors)
}

func TestCatalogSchema(t *testing.T) {
	var schema = CatalogJSONSchema()
	require.True(t, len(schema) > 100)
}
