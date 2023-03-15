package runtime

import (
	"testing"

	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/protocol"
)

func TestRemoveOldOpsJournalAckIntents(t *testing.T) {
	cp := pf.Checkpoint{
		AckIntents: map[protocol.Journal][]byte{
			protocol.Journal("a/good/journal"):                                                   []byte("keep1"),
			protocol.Journal("ops/estuary/logs/kind=materialization/name=materialization"):       []byte("drop1"),
			protocol.Journal("ops/estuary/stats/kind=capture/name=capture"):                      []byte("drop2"),
			protocol.Journal("ops/some.tenant/logs/kind=derivation/name=something"):              []byte("drop3"),
			protocol.Journal("ops/other-tenant/stats/kind=materialization/name=someting/else"):   []byte("drop4"),
			protocol.Journal("ops.us-central1.v1/stats/kind=materialization/name=something"):     []byte("keep2"),
			protocol.Journal("ops.us-central1.v1/logs/kind=capture/name=another"):                []byte("keep3"),
			protocol.Journal("hello/ops/estuary/logs/kind=materialization/name=materialization"): []byte("keep4"),
		},
	}

	removeOldOpsJournalAckIntents(cp.AckIntents)

	want := pf.Checkpoint{
		AckIntents: map[protocol.Journal][]byte{
			protocol.Journal("a/good/journal"):                                                   []byte("keep1"),
			protocol.Journal("ops.us-central1.v1/stats/kind=materialization/name=something"):     []byte("keep2"),
			protocol.Journal("ops.us-central1.v1/logs/kind=capture/name=another"):                []byte("keep3"),
			protocol.Journal("hello/ops/estuary/logs/kind=materialization/name=materialization"): []byte("keep4"),
		},
	}

	require.Equal(t, want, cp)
}

func TestCaptureStats(t *testing.T) {
	// Simulate a capture with two bindings, where only one of them participated in the transaction.
	var captureSpec = pf.CaptureSpec{
		Capture: pf.Capture("test/capture"),
		Bindings: []*pf.CaptureSpec_Binding{
			{
				Collection: pf.CollectionSpec{
					Collection: pf.Collection("test/collectionA"),
				},
			},
			{
				Collection: pf.CollectionSpec{
					Collection: pf.Collection("test/collectionA"),
				},
			},
		},
	}
	var subject = Capture{
		taskTerm: taskTerm{
			StatsFormatter: newTestFormatter("test/capture", "capture"),
		},
		spec: captureSpec,
	}
	var inputs = []*pf.CombineAPI_Stats{
		nil,
		{
			Right: &pf.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: &pf.DocsAndBytes{
				Docs:  5,
				Bytes: 5555,
			},
		},
	}
	var actual = subject.captureStats(inputs)

	var expect = map[string]ops.CaptureBindingStats{
		"test/collectionA": {
			Right: ops.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
			Out: ops.DocsAndBytes{
				Docs:  5,
				Bytes: 5555,
			},
		},
	}
	assertEventMeta(t, actual, &captureSpec, "capture")
	require.Equal(t, expect, actual.Capture)

	// Test where stats exist for multiple bindings that each reference the same collection
	// and assert that the result is the sum.
	inputs = []*pf.CombineAPI_Stats{
		{
			Right: &pf.DocsAndBytes{
				Docs:  1,
				Bytes: 1111,
			},
			Out: &pf.DocsAndBytes{
				Docs:  2,
				Bytes: 2222,
			},
		},
		{
			Right: &pf.DocsAndBytes{
				Docs:  3,
				Bytes: 3333,
			},
			Out: &pf.DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
		},
	}
	actual = subject.captureStats(inputs)

	expect = map[string]ops.CaptureBindingStats{
		"test/collectionA": {
			Right: ops.DocsAndBytes{
				Docs:  4,
				Bytes: 4444,
			},
			Out: ops.DocsAndBytes{
				Docs:  6,
				Bytes: 6666,
			},
		},
	}
	assertEventMeta(t, actual, &captureSpec, "capture")
	require.Equal(t, expect, actual.Capture)
}
