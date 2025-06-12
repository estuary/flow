package broker

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestListCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	defer func(v int) { maxJournalsPerListResponse = v }(maxJournalsPerListResponse)
	maxJournalsPerListResponse = 2

	// Create a fixture of JournalSpecs which we'll list.
	var fragSpec = pb.JournalSpec_Fragment{
		Length:           1024,
		RefreshInterval:  time.Second,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}
	var specA = &pb.JournalSpec{
		Name:        "journal/1/A",
		LabelSet:    pb.MustLabelSet("foo", "bar"),
		Replication: 1,
		Fragment:    fragSpec,
	}
	var specB = &pb.JournalSpec{
		Name:        "journal/2/B",
		LabelSet:    pb.MustLabelSet("bar", "baz"),
		Replication: 1,
		Fragment:    fragSpec,
	}
	var specC = &pb.JournalSpec{
		Name:        "journal/1/C",
		Replication: 1,
		Fragment:    fragSpec,
	}

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var fixtureRevision int64
	{
		var resp, err = broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: specA},
				{Upsert: specB},
				{Upsert: specC},
			},
		})
		require.NoError(t, err)
		require.Equal(t, pb.Status_OK, resp.Status)
		fixtureRevision = resp.Header.Etcd.Revision
	}

	// Assign |specC| to |bk|, to verify its returned non-empty Route.
	var rev = mustKeyValues(t, etcd, map[string]string{
		allocator.AssignmentKey(broker.ks, allocator.Assignment{
			ItemID:       "journal/1/C",
			MemberZone:   "local",
			MemberSuffix: "broker",
			Slot:         1,
		}): ""})

	broker.ks.Mu.RLock()
	require.NoError(t, broker.ks.WaitForRevision(ctx, rev))
	broker.ks.Mu.RUnlock()

	var verify = func(stream pb.Journal_ListClient, expect ...*pb.JournalSpec) {
		var resp, err = stream.Recv()
		require.NoError(t, err)
		require.Equal(t, pb.Status_OK, resp.Status)

		// Accumulate all streamed responses.
		for {
			var next, err = stream.Recv()
			if err == io.EOF || err == nil && len(next.Journals) == 0 {
				break // End of stream or snapshot.
			}
			require.NoError(t, err)
			resp.Journals = append(resp.Journals, next.Journals...)
		}
		require.Len(t, resp.Journals, len(expect))

		for i, exp := range expect {
			require.Equal(t, *exp, resp.Journals[i].Spec)

			require.Equal(t, fixtureRevision, resp.Journals[i].CreateRevision)
			require.Equal(t, fixtureRevision, resp.Journals[i].ModRevision)

			if exp == specC {
				require.Equal(t, []pb.ProcessSpec_ID{{Zone: "local", Suffix: "broker"}},
					resp.Journals[i].Route.Members)
				require.Equal(t, []pb.Endpoint{broker.srv.Endpoint()},
					resp.Journals[i].Route.Endpoints)
			} else {
				require.Nil(t, resp.Journals[i].Route.Members)
				require.Nil(t, resp.Journals[i].Route.Endpoints)
			}
		}
	}

	// Case: Empty selector returns all journals.
	var stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{},
	})
	require.NoError(t, err)
	verify(stream, specA, specC, specB)

	// Case: Unmatched include selector returns no journals.
	stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("missing", "label")},
	})
	require.NoError(t, err)
	verify(stream)

	// Case: Exclude on label.
	stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Exclude: pb.MustLabelSet("foo", "")},
	})
	require.NoError(t, err)
	verify(stream, specC, specB)

	// Case: Meta-label "name" selects journals by name.
	stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", "journal/2/B")},
	})
	require.NoError(t, err)
	verify(stream, specB)

	// Case: Meta-label "name:prefix" selects journals by name prefix.
	stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name:prefix", "journal/1")},
	})
	require.NoError(t, err)
	verify(stream, specA, specC)

	// Case: legacy meta-label "prefix" also selects journals by name prefix.
	stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", "journal/1/")},
	})
	require.NoError(t, err)
	verify(stream, specA, specC)

	// Case: Errors on request validation error.
	stream, err = broker.client().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", "invalid/because/missing/trailing/slash")},
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Regexp(t, `.* Selector.Include.Labels\["prefix"\]: expected trailing '/' (.*)`, err)

	// Case: Insufficient claimed capability.
	stream, err = broker.client().List(
		pb.WithClaims(ctx, pb.Claims{Capability: pb.Capability_APPEND}),
		&pb.ListRequest{Selector: pb.LabelSelector{}})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.ErrorContains(t, err, "authorization is missing required LIST capability")

	// Case: Insufficient claimed selector.
	stream, err = broker.client().List(
		pb.WithClaims(ctx,
			pb.Claims{
				Capability: pb.Capability_LIST,
				Selector:   pb.MustLabelSelector("name=something/else"),
			}),
		&pb.ListRequest{Selector: pb.LabelSelector{}})
	require.NoError(t, err)
	verify(stream) // All journals filtered by the claims selector.

	// Case: streaming watch of a prefix.
	var cancelCtx, cancel = context.WithCancel(ctx)
	stream, err = broker.client().List(cancelCtx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name:prefix", "journal/1/")},
		Watch:    true,
	})
	require.NoError(t, err)
	verify(stream, specA, specC)

	// Delete a journal that's not part of the listing.
	resp, err := broker.client().Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Delete: specB.Name, ExpectModRevision: fixtureRevision},
		},
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	// Delete a journal that IS part of the listing.
	resp, err = broker.client().Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Delete: specA.Name, ExpectModRevision: fixtureRevision},
		},
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	// Expect we see a new snapshot, this time having only C.
	verify(stream, specC)

	// Delete final listed journal.
	resp, err = broker.client().Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Delete: specC.Name, ExpectModRevision: fixtureRevision},
		},
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	// Expect we see an empty snapshot.
	verify(stream)

	// Expect to read no further responses upon cancellation.
	cancel()
	_, err = stream.Recv()
	require.Equal(t, "rpc error: code = Canceled desc = context canceled", err.Error())

	broker.cleanup()
}

func TestApplyCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Fixtures for use in the test.
	var fragSpec = pb.JournalSpec_Fragment{
		Length:           1024,
		RefreshInterval:  time.Second,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}
	var specA = pb.JournalSpec{
		Name:        "journal/A",
		Replication: 1,
		Fragment:    fragSpec,
	}
	var specB = pb.JournalSpec{
		Name:        "journal/B",
		Replication: 1,
		Fragment:    fragSpec,
	}
	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	var verifyAndFetchRev = func(name pb.Journal, expect pb.JournalSpec) int64 {
		var stream, err = broker.client().List(ctx, &pb.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", name.String())},
		})
		require.NoError(t, err)
		resp, err := stream.Recv()
		require.NoError(t, err)

		require.Equal(t, pb.Status_OK, resp.Status)
		require.Equal(t, expect, resp.Journals[0].Spec)
		return resp.Journals[0].ModRevision
	}

	var must = func(r *pb.ApplyResponse, err error) *pb.ApplyResponse {
		require.NoError(t, err)
		return r
	}

	// Case: Create new specs A & B.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specA},
				{Upsert: &specB},
			},
		})).Status)

	// Case: Update existing spec B.
	var origSpecB = specB
	specB.Labels = append(specB.Labels, pb.Label{Name: "foo", Value: "bar"})

	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specB, ExpectModRevision: verifyAndFetchRev("journal/B", origSpecB)},
			},
		})).Status)

	// Case: Delete existing spec A.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Delete: "journal/A", ExpectModRevision: verifyAndFetchRev("journal/A", specA)},
			},
		})).Status)

	// Case: Deletion at wrong revision fails.
	require.Equal(t, pb.Status_ETCD_TRANSACTION_FAILED,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Delete: "journal/B", ExpectModRevision: verifyAndFetchRev("journal/B", specB) - 1},
			},
		})).Status)

	// Case: Update at wrong revision fails.
	require.Equal(t, pb.Status_ETCD_TRANSACTION_FAILED,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specB, ExpectModRevision: verifyAndFetchRev("journal/B", specB) - 1},
			},
		})).Status)

	// Case: Update with explicit revision of -1 succeeds.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: &specB, ExpectModRevision: -1},
			},
		})).Status)

	// Case: Deletion with explicit revision of -1 succeeds.
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Delete: "journal/B", ExpectModRevision: -1},
			},
		})).Status)

	// Case: Invalid requests fail with an error.
	var _, err = broker.client().Apply(pb.WithClaims(ctx, allClaims), &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{{Delete: "invalid journal name"}},
	})
	require.Regexp(t, `.* Changes\[0\].Delete: not a valid token \(invalid journal name\)`, err)

	// Case: Insufficient claimed capability.
	_, err = broker.client().Apply(
		pb.WithClaims(ctx, pb.Claims{Capability: pb.Capability_READ}),
		&pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{{Delete: "journal/A"}},
		})
	require.ErrorContains(t, err, "authorization is missing required APPLY capability")

	var ctxNarrowClaims = pb.WithClaims(ctx,
		pb.Claims{
			Capability: pb.Capability_APPLY,
			Selector:   pb.MustLabelSelector("name=something/else"),
		})

	// Case: Insufficient claimed selector on delete.
	_, err = broker.client().Apply(
		ctxNarrowClaims,
		&pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{{Delete: "journal/A", ExpectModRevision: -1}},
		})
	require.ErrorContains(t, err, "not authorized to journal/A")

	// Case: Insufficient claimed selector on upsert.
	_, err = broker.client().Apply(
		ctxNarrowClaims,
		&pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{{Upsert: &specB}},
		})
	require.ErrorContains(t, err, "not authorized to journal/B")

	// Case: Upsert with labels requires claims to match all labels (not just name).
	// Create a context with claims that require env=staging
	var ctxRequireStagingEnv = pb.WithClaims(ctx,
		pb.Claims{
			Capability: pb.Capability_APPLY,
			Selector:   pb.MustLabelSelector("name=something/else,env=staging"),
		})
	var specWithLabels = pb.JournalSpec{
		Name:        "something/else",                     // This matches the name in claims
		LabelSet:    pb.MustLabelSet("env", "production"), // But env=production doesn't match env=staging
		Replication: 1,
		Fragment:    fragSpec,
	}
	_, err = broker.client().Apply(
		ctxRequireStagingEnv,
		&pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{{Upsert: &specWithLabels}},
		})
	require.ErrorContains(t, err, "not authorized to something/else")

	// Case: Upsert succeeds when claims selector matches all labels.
	var ctxMatchingClaims = pb.WithClaims(ctx,
		pb.Claims{
			Capability: pb.Capability_APPLY,
			Selector:   pb.MustLabelSelector("name=something/else,env=production"),
		})
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(
			ctxMatchingClaims,
			&pb.ApplyRequest{
				Changes: []pb.ApplyRequest_Change{{Upsert: &specWithLabels}},
			})).Status)

	// Case: Delete still only requires name to match (not other labels).
	require.Equal(t, pb.Status_OK,
		must(broker.client().Apply(
			ctxNarrowClaims,
			&pb.ApplyRequest{
				Changes: []pb.ApplyRequest_Change{{Delete: "something/else", ExpectModRevision: -1}},
			})).Status)

	broker.cleanup()
}

var allClaims = pb.Claims{
	Capability: pb.Capability_ALL,  // All APIs.
	Selector:   pb.LabelSelector{}, // Match anything.
}
