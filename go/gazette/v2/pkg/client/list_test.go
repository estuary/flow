package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type ListSuite struct{}

func (s *ListSuite) TestListAllJournalsCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var selector = pb.LabelSelector{Include: pb.MustLabelSet("foo", "bar")}

	var mk = buildListResponseFixture // Alias.

	// Case: ListAllJournals submits multiple requests and joins their results.
	var expect = []pb.ListRequest{
		{Selector: selector, PageLimit: 10},
		{Selector: selector, PageLimit: 10, PageToken: "tok-1"},
		{Selector: selector, PageLimit: 10, PageToken: "tok-2"},
	}
	var hdr = *buildHeaderFixture(broker)
	var responses = []pb.ListResponse{
		{Header: hdr, Journals: mk("part-one"), NextPageToken: "tok-1"},
		{Header: hdr, Journals: mk("part-two"), NextPageToken: "tok-2"},
		{Header: hdr, Journals: mk("part-three")},
	}

	broker.ListFunc = func(_ context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
		c.Check(*req, gc.DeepEquals, expect[0])
		var resp = &responses[0]
		expect, responses = expect[1:], responses[1:]
		return resp, nil
	}

	var rc = NewRouteCache(10, time.Hour)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), rc)
	var resp, err = ListAllJournals(ctx, rjc, pb.ListRequest{Selector: selector, PageLimit: 10})

	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.ListResponse{
		Header:   hdr,
		Journals: mk("part-one", "part-two", "part-three"),
	})
	c.Check(rc.cache.Len(), gc.Equals, 3)

	// Case: A single RPC is required.
	expect = []pb.ListRequest{{Selector: selector}}
	responses = []pb.ListResponse{{Header: hdr, Journals: mk("only/one")}}

	resp, err = ListAllJournals(context.Background(), broker.MustClient(), pb.ListRequest{Selector: selector})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.ListResponse{Header: hdr, Journals: mk("only/one")})

	// Case: It fails on response validation failure.
	expect = []pb.ListRequest{{Selector: selector}}
	responses = []pb.ListResponse{{Header: hdr, Journals: mk("invalid name")}}

	_, err = ListAllJournals(context.Background(), broker.MustClient(), pb.ListRequest{Selector: selector})
	c.Check(err, gc.ErrorMatches, `Journals\[0\].Spec.Name: not a valid token \(invalid name\)`)

	// Case: It fails on non-OK status.
	expect = []pb.ListRequest{{Selector: selector}}
	responses = []pb.ListResponse{{Header: hdr, Status: pb.Status_WRONG_ROUTE}}

	_, err = ListAllJournals(context.Background(), broker.MustClient(), pb.ListRequest{Selector: selector})
	c.Check(err, gc.ErrorMatches, `WRONG_ROUTE`)

	// Case: It surfaces context.Canceled, rather than a gRPC error.
	cancel()
	_, err = ListAllJournals(ctx, rjc, pb.ListRequest{Selector: selector, PageLimit: 10})
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *ListSuite) TestPolledList(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var mk = buildListResponseFixture // Alias.

	var fixture = pb.ListResponse{
		Header:   *buildHeaderFixture(broker),
		Journals: mk("part-one", "part-two"),
	}

	var callCh = make(chan struct{}, 1)
	defer close(callCh)

	broker.ListFunc = func(_ context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
		<-callCh
		return &fixture, nil
	}

	// Expect NewPolledList calls ListAllJournals once, and List is prepared before return.
	callCh <- struct{}{}

	var pl, err = NewPolledList(ctx, broker.MustClient(), 5*time.Millisecond, pb.ListRequest{})
	c.Check(err, gc.IsNil)
	c.Check(pl.List(), gc.DeepEquals, &fixture)

	// Alter the fixture. List will eventually reflect it, after being given a chance to refresh.
	fixture.Journals = mk("part-one", "part-two", "part-three")
	c.Check(pl.List(), gc.Not(gc.DeepEquals), &fixture)

	for i := 0; i != 3; i++ {
		callCh <- struct{}{} // Wait until |fixture| is certain to be applied.
	}
	c.Check(pl.List(), gc.DeepEquals, &fixture)
}

func (s *ListSuite) TestListAllFragments(c *gc.C) {
	var ctx = context.Background()
	var broker = teststub.NewBroker(c, ctx)
	var hdr = buildHeaderFixture(broker)
	var fixture1 = &pb.FragmentsResponse{
		Header:        *hdr,
		Fragments:     buildSignedFragmentsFixture("a/journal", 0),
		NextPageToken: 30,
	}
	var fixture2 = &pb.FragmentsResponse{
		Header:        *hdr,
		Fragments:     buildSignedFragmentsFixture("a/journal", 30),
		NextPageToken: 0,
	}

	broker.FragmentsFunc = func(_ context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		switch req.NextPageToken {
		case 0:
			return fixture1, nil
		case 30:
			return fixture2, nil
		default:
			return nil, errors.New("should not be called")
		}
	}

	var client = pb.NewRoutedJournalClient(broker.MustClient(), NewRouteCache(2, time.Hour))
	var req = pb.FragmentsRequest{Journal: pb.Journal("a/journal")}
	// Case: properly coalesce fragment pages
	var resp, err = ListAllFragments(ctx, client, req)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Header:    *hdr,
		Fragments: append(fixture1.Fragments, fixture2.Fragments...),
	})

	// Case: broker non-OK status
	broker.FragmentsFunc = func(_ context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		return &pb.FragmentsResponse{
			Header: *hdr,
			Status: pb.Status_JOURNAL_NOT_FOUND,
		}, nil
	}
	resp, err = ListAllFragments(ctx, client, req)
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.ErrorMatches, pb.Status_JOURNAL_NOT_FOUND.String())

	// Case: broker error
	broker.FragmentsFunc = func(_ context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		return nil, errors.New("something has gone wrong")
	}
	resp, err = ListAllFragments(ctx, client, req)
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = something has gone wrong`)

	// Case: invalid response
	broker.FragmentsFunc = func(_ context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		return &pb.FragmentsResponse{
			Header: *hdr,
			Status: 1000,
		}, nil
	}
	resp, err = ListAllFragments(ctx, client, req)
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.ErrorMatches, `Status: invalid status \(1000\)`)
}

func (s *ListSuite) TestApplyJournalsInBatches(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var client = pb.NewRoutedJournalClient(broker.MustClient(), NewRouteCache(2, time.Hour))

	var hdr = buildHeaderFixture(broker)
	// Case: size is 0. All changes are submitted.
	var fixture = buildApplyReqFixtue()
	var expected = &pb.ApplyResponse{
		Status: pb.Status_OK,
		Header: *hdr,
	}
	broker.ApplyFunc = func(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
		c.Check(req, gc.DeepEquals, fixture)
		return expected, nil
	}
	resp, err := ApplyJournalsInBatches(ctx, client, fixture, 0)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// Case: size == len(req.Changes). All changes are submitted.
	resp, err = ApplyJournalsInBatches(ctx, client, fixture, 3)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// Case: size < len(req.Changes). Changes are batched.
	var iter = 0
	broker.ApplyFunc = func(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
		c.Check(req, gc.DeepEquals, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{Upsert: fixture.Changes[iter].Upsert, ExpectModRevision: 1},
			},
		})
		iter++
		return expected, nil
	}
	resp, err = ApplyJournalsInBatches(ctx, client, fixture, 1)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expected)

	// Case: empty list of changes.
	broker.ApplyFunc = func(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
		c.Error("should not be called")
		return nil, nil
	}
	resp, err = ApplyJournalsInBatches(ctx, client, &pb.ApplyRequest{}, 1)
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.ApplyResponse{})

	// Case: Return Error from backend.
	broker.ApplyFunc = func(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
		return nil, errors.New("something has gone wrong")
	}
	resp, err = ApplyJournalsInBatches(ctx, client, fixture, 1)
	c.Check(err, gc.ErrorMatches, "rpc error: code = Unknown desc = something has gone wrong")

	// Case: Status !OK mapped as an error.
	broker.ApplyFunc = func(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
		return &pb.ApplyResponse{
			Status: pb.Status_ETCD_TRANSACTION_FAILED,
			Header: *hdr,
		}, nil
	}
	resp, err = ApplyJournalsInBatches(ctx, client, fixture, 1)
	c.Check(err.Error(), gc.Matches, pb.Status_ETCD_TRANSACTION_FAILED.String())

	// Case: Validation error mapped as error.
	broker.ApplyFunc = func(ctx context.Context, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
		return &pb.ApplyResponse{
			Status: pb.Status_ETCD_TRANSACTION_FAILED,
		}, nil
	}
	resp, err = ApplyJournalsInBatches(ctx, client, fixture, 1)
	c.Check(err, gc.ErrorMatches, `Header.Route: invalid Primary \(0; expected -1 <= Primary < 0\)`)
}

func buildApplyReqFixtue() *pb.ApplyRequest {
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

	return &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{Upsert: specA, ExpectModRevision: 1},
			{Upsert: specB, ExpectModRevision: 1},
		},
	}
}

func buildListResponseFixture(names ...pb.Journal) (out []pb.ListResponse_Journal) {
	for _, n := range names {
		out = append(out, pb.ListResponse_Journal{
			Spec: pb.JournalSpec{
				Name:        n,
				Replication: 1,
				Fragment: pb.JournalSpec_Fragment{
					Length:           1 << 24, // 16MB.
					CompressionCodec: pb.CompressionCodec_SNAPPY,
					RefreshInterval:  time.Minute,
					Retention:        time.Hour,
				},
			},
			ModRevision: 1234,
			Route: pb.Route{
				Members: []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
				Primary: 0,
			},
		})
	}
	return
}

func buildSignedFragmentsFixture(journal pb.Journal, startOffset int64) []pb.FragmentsResponse_SignedFragment {
	return []pb.FragmentsResponse_SignedFragment{
		{
			Fragment: pb.Fragment{
				Journal:          journal,
				Begin:            startOffset,
				End:              startOffset + 10,
				ModTime:          startOffset,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: fmt.Sprintf("valid_url_%v", startOffset),
		},
		{
			Fragment: pb.Fragment{
				Journal:          journal,
				Begin:            startOffset + 20,
				End:              startOffset + 30,
				ModTime:          startOffset + 10,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: fmt.Sprintf("valid_url_%v_%v", startOffset, "_20"),
		},
	}
}

var _ = gc.Suite(&ListSuite{})
