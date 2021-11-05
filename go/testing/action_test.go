package testing

import (
	"context"
	"testing"
	"time"

	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/mock"
)

func TestTestCaseExecution(t *testing.T) {
	var derivations = derivationsFixture(
		transformFixture("A", "A-to-B-fast", "B", 0),
		transformFixture("A", "A-to-B-slow", "B", 3),
		transformFixture("A", "A-to-Y", "Y", 2),
		transformFixture("A", "A-to-Z", "Z", 5),
	)
	var test = &pf.TestSpec{
		Steps: []pf.TestSpec_Step{
			{StepType: pf.TestSpec_Step_INGEST, Collection: "A"},
			{StepType: pf.TestSpec_Step_VERIFY, Collection: "B"},
		},
	}
	var graph = NewGraph(nil, derivations, nil)

	var driver = new(mockDriver)

	// Initial ingestion into A.
	driver.On("Ingest", test, 0).Return(clockFixtureOne(1, "A/data", 1), nil).Once()

	// Stat of B from "A-to-B-fast" is immediately ready.
	driver.On("Stat", PendingStat{
		TaskName:    "B",
		ReadyAt:     0,
		ReadThrough: clockFixtureOne(1, "A/data;derive/B/A-to-B-fast", 1),
	}).Return(
		clockFixture(1, nil, nil),
		clockFixture(1, nil, nil),
		nil,
	).Once()

	// We must still advance until transform "A-to-B-slow" can run.
	driver.On("Advance", TestTime(2*time.Second)).Return(nil).Once()

	// "A-to-Y" unblocks first.
	driver.On("Stat", PendingStat{
		TaskName:    "Y",
		ReadyAt:     TestTime(2 * time.Second),
		ReadThrough: clockFixtureOne(1, "A/data;derive/Y/A-to-Y", 1),
	}).Return(
		clockFixture(1, nil, nil),
		clockFixture(1, nil, nil),
		nil,
	).Once()

	driver.On("Advance", TestTime(time.Second)).Return(nil).Once()

	// Now "A-to-B-slow" unblocks.
	driver.On("Stat", PendingStat{
		TaskName:    "B",
		ReadyAt:     TestTime(3 * time.Second),
		ReadThrough: clockFixtureOne(1, "A/data;derive/B/A-to-B-slow", 1),
	}).Return(
		clockFixture(1, nil, nil),
		clockFixture(1, nil, nil),
		nil,
	).Once()

	// We may verify B, as no dependent stats remain.
	driver.On("Verify", test, 1,
		clockFixture(0, nil, nil),
		clockFixtureOne(1, "A/data", 1),
	).Return(nil).Once()

	// No test steps remain, but we must still drain pending stats.
	driver.On("Advance", TestTime(2*time.Second)).Return(nil).Once()

	// "A-to-Z" unblocks.
	driver.On("Stat", PendingStat{
		TaskName:    "Z",
		ReadyAt:     TestTime(5 * time.Second),
		ReadThrough: clockFixtureOne(1, "A/data;derive/Z/A-to-Z", 1),
	}).Return(
		clockFixture(1, nil, nil),
		clockFixture(1, nil, nil),
		nil,
	).Once()

	RunTestCase(context.Background(), graph, driver, test)
	driver.AssertExpectations(t)
}

type mockDriver struct {
	mock.Mock
}

var _ Driver = &mockDriver{}

func (d *mockDriver) Stat(ctx context.Context, in PendingStat) (readThrough *Clock, writeAt *Clock, _ error) {
	var args = d.Called(in)
	return args.Get(0).(*Clock), args.Get(1).(*Clock), args.Error(2)
}

func (d *mockDriver) Ingest(ctx context.Context, test *pf.TestSpec, testStep int) (writeAt *Clock, _ error) {
	var args = d.Called(test, testStep)
	return args.Get(0).(*Clock), args.Error(1)
}

func (d *mockDriver) Verify(ctx context.Context, test *pf.TestSpec, testStep int, from, to *Clock) error {
	var args = d.Called(test, testStep, from, to)
	return args.Error(0)
}

func (d *mockDriver) Advance(ctx context.Context, in TestTime) error {
	var args = d.Called(in)
	return args.Error(0)
}
