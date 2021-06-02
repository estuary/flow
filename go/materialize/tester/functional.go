package tester

import (
	"testing"

	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// RunFunctionalTest executes a basic functional test of a materialization driver.
func RunFunctionalTest(t *testing.T, fixture *Fixture) {
	TestSetup(t, fixture)
	var request *pm.TransactionRequest = nil
	var stream, _, err = fixture.OpenTransactions(&request, nil)
	require.NoError(t, err, "starting transactions")

	var testDocs = fixture.generator.generateDocs(3)
	prepared, err := fixture.LoadDocuments(stream, &request, 1, testDocs)
	require.NoError(t, err, "loading initial documents")
	log.WithField("driverCheckpoint", prepared.DriverCheckpointJson).Debug("verified first loads")

	err = fixture.StoreDocuments(stream, &request, testDocs)
	require.NoError(t, err, "storing initial documents")
	log.Debug("Store committed")

	for _, newDoc := range fixture.generator.generateDocs(3) {
		testDocs = append(testDocs, newDoc)
	}
	prepared, err = fixture.LoadDocuments(stream, &request, 2, testDocs)
	require.NoError(t, err, "loading second set of documents")
	log.WithField("driverCheckpoint", prepared.DriverCheckpointJson).Debug("verified second loads")

	for _, doc := range testDocs[:3] {
		fixture.generator.updateValues(doc)
	}
	err = fixture.StoreDocuments(stream, &request, testDocs)
	require.NoError(t, err, "storing second batch of documents")
	err = stream.CloseSend()
	require.NoError(t, err, "closing stream send")

	stream, opened, err := fixture.OpenTransactions(&request, prepared.DriverCheckpointJson)
	// Assert that the Opened message contains the most recently stored Flow Checkpoint value
	require.Equal(t, flowCheckpointBytes(2), opened.FlowCheckpoint)

	prepared, err = fixture.LoadDocuments(stream, &request, 3, testDocs)
	err = stream.Send(&pm.TransactionRequest{Commit: &pm.TransactionRequest_Commit{}})
	require.NoError(t, err, "failed to send commit")
	stream.CloseSend()
	finalResponse, err := stream.Recv()
	require.NoError(t, err, "failed to receive final message")
	require.NotNil(t, finalResponse.Committed, "expected a final non-nil Committed message")
}
