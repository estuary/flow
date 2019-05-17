package broker

import (
	"time"

	"github.com/gazette/gazette/v2/allocator"
	"github.com/gazette/gazette/v2/etcdtest"
	pb "github.com/gazette/gazette/v2/protocol"
	gc "github.com/go-check/check"
)

type ListApplySuite struct{}

func (s *ListApplySuite) TestListCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

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

	var bk = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var rjc = pb.NewRoutedJournalClient(bk.MustClient(), pb.NoopDispatchRouter{})
	var ctx = pb.WithDispatchDefault(tf.ctx)

	{
		var resp, err = rjc.Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: specA},
				{Upsert: specB},
				{Upsert: specC},
			},
		})
		c.Assert(err, gc.IsNil)
		c.Assert(resp.Status, gc.Equals, pb.Status_OK)
	}

	// Assign |specC| to |bk|, to verify its returned non-empty Route.
	mustKeyValues(c, tf, map[string]string{
		allocator.AssignmentKey(tf.ks, allocator.Assignment{
			ItemID:       "journal/1/C",
			MemberZone:   "local",
			MemberSuffix: "broker",
			Slot:         1,
		}): ""})

	var verify = func(resp *pb.ListResponse, expect ...*pb.JournalSpec) {
		c.Check(resp.Status, gc.Equals, pb.Status_OK)
		c.Assert(resp.Journals, gc.HasLen, len(expect))

		for i, exp := range expect {
			c.Check(resp.Journals[i].Spec, gc.DeepEquals, *exp)

			if exp == specC {
				c.Check(resp.Journals[i].Route.Members, gc.DeepEquals,
					[]pb.ProcessSpec_ID{{Zone: "local", Suffix: "broker"}})
				c.Check(resp.Journals[i].Route.Endpoints, gc.DeepEquals,
					[]pb.Endpoint{bk.Endpoint()})
			} else {
				c.Check(resp.Journals[i].Route.Members, gc.IsNil)
				c.Check(resp.Journals[i].Route.Endpoints, gc.IsNil)
			}
		}
	}

	// Case: Empty selector returns all shards.
	var resp, err = rjc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specA, specC, specB)

	// Case: Exclude on label.
	resp, err = rjc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Exclude: pb.MustLabelSet("foo", "")},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specC, specB)

	// Case: Meta-label "name" selects journals by name.
	resp, err = rjc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", "journal/2/B")},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specB)

	// Case: Meta-label "prefix" selects journals by name prefix.
	resp, err = rjc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", "journal/1/")},
	})
	c.Check(err, gc.IsNil)
	verify(resp, specA, specC)

	// Case: Errors on request validation error.
	_, err = rjc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", "invalid/because/missing/trailing/slash")},
	})
	c.Check(err, gc.ErrorMatches, `.* Selector.Include.Labels\["prefix"\]: expected trailing '/' (.*)`)

	etcdtest.Cleanup() // We wrote keys outside of |bk|'s lease, and must manually cleanup.
}

func (s *ListApplySuite) TestApplyCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

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

	var bk = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var rjc = pb.NewRoutedJournalClient(bk.MustClient(), pb.NoopDispatchRouter{})
	var ctx = pb.WithDispatchDefault(tf.ctx)

	var verifyAndFetchRev = func(name pb.Journal, expect pb.JournalSpec) int64 {
		var resp, err = rjc.List(ctx, &pb.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", name.String())},
		})
		c.Assert(err, gc.IsNil)
		c.Assert(resp.Status, gc.Equals, pb.Status_OK)
		c.Assert(resp.Journals[0].Spec, gc.DeepEquals, expect)
		return resp.Journals[0].ModRevision
	}

	var must = func(r *pb.ApplyResponse, err error) *pb.ApplyResponse {
		c.Assert(err, gc.IsNil)
		return r
	}

	// Case: Create new specs A & B.
	c.Check(must(rjc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Upsert: &specA},
			{Upsert: &specB},
		},
	})).Status, gc.Equals, pb.Status_OK)

	// Case: Update existing spec B.
	var origSpecB = specB
	specB.Labels = append(specB.Labels, pb.Label{Name: "foo", Value: "bar"})

	c.Check(must(rjc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Upsert: &specB, ExpectModRevision: verifyAndFetchRev("journal/B", origSpecB)},
		},
	})).Status, gc.Equals, pb.Status_OK)

	// Case: Delete existing spec A.
	c.Check(must(rjc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Delete: "journal/A", ExpectModRevision: verifyAndFetchRev("journal/A", specA)},
		},
	})).Status, gc.Equals, pb.Status_OK)

	// Case: Deletion at wrong revision fails.
	c.Check(must(rjc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Delete: "journal/B", ExpectModRevision: verifyAndFetchRev("journal/B", specB) - 1},
		},
	})).Status, gc.Equals, pb.Status_ETCD_TRANSACTION_FAILED)

	// Case: Update at wrong revision fails.
	c.Check(must(rjc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Upsert: &specB, ExpectModRevision: verifyAndFetchRev("journal/B", specB) - 1},
		},
	})).Status, gc.Equals, pb.Status_ETCD_TRANSACTION_FAILED)

	// Case: Invalid requests fail with an error.
	var _, err = rjc.Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{{Delete: "invalid journal name"}},
	})
	c.Check(err, gc.ErrorMatches, `.* Changes\[0\].Delete: not a valid token \(invalid journal name\)`)

	etcdtest.Cleanup() // We wrote keys outside of |bk|'s lease, and must manually cleanup.
}

var _ = gc.Suite(&ListApplySuite{})
