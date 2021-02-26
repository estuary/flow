// +build pgdrivertest

package test

import (
	"testing"

	"github.com/estuary/flow/go/materialize/tester"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPostgresDriver(t *testing.T) {
	var endpointConfig = `{
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "testpgpass",
        "dbName": "postgres",
        "table": "material_test"
    }`
	var fixture, err = tester.NewFixture(pf.EndpointType_POSTGRESQL, endpointConfig)
	require.NoError(t, err, "creating test fixture")
	tester.RunFunctionalTest(t, fixture)
}
