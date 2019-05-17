package broker

import (
	"context"
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/client"
	"go.gazette.dev/core/fragment"
	pb "go.gazette.dev/core/protocol"
)

type FragmentsSuite struct{}

func (s *FragmentsSuite) TestResolutionCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()
	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	var rc = client.NewRouteCache(10, time.Hour)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), rc)
	var fixture = buildFragmentsFixture()
	var ctx = pb.WithDispatchDefault(tf.ctx)

	// Case: Missing journal name.
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "an/missing/journal", mayProxy: true})
	var resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal: "a/missing/journal",
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status: pb.Status_JOURNAL_NOT_FOUND,
		Header: res.Header,
	})

	// Case: Read from a write only journal.
	newTestJournal(c, tf, pb.JournalSpec{Name: "write/only/journal", Replication: 1, Flags: pb.JournalSpec_O_WRONLY}, peer.id)
	res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "write/only/journal", mayProxy: true})
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal: "write/only/journal",
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status: pb.Status_NOT_ALLOWED,
		Header: res.Header,
	})

	// Case: Proxy request to peer.
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)
	res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", mayProxy: true})
	peer.ListFragmentsFunc = func(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		c.Check(req, gc.DeepEquals, &pb.FragmentsRequest{
			Header:        &res.Header,
			Journal:       "a/journal",
			BeginModTime:  time.Unix(0, 0).Unix(),
			EndModTime:    time.Unix(0, 0).Unix(),
			NextPageToken: 0,
			DoNotProxy:    false,
		})
		return &pb.FragmentsResponse{
			Status:        pb.Status_OK,
			Header:        res.Header,
			Fragments:     fixture,
			NextPageToken: fixture[5].Spec.End,
		}, nil
	}

	resp, err = broker.MustClient().ListFragments(ctx, &pb.FragmentsRequest{
		Journal: "a/journal",
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		Fragments:     fixture,
		NextPageToken: fixture[5].Spec.End,
	})
}

func (s *FragmentsSuite) TestListFragments(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var rc = client.NewRouteCache(10, time.Hour)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), rc)
	var oneSec = time.Second

	// Case: Request validation error.
	var ctx = pb.WithDispatchDefault(tf.ctx)
	var resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		BeginModTime:  50,
		EndModTime:    40,
		NextPageToken: 0,
		DoNotProxy:    false,
	})
	c.Check(err, gc.ErrorMatches, `.* invalid EndModTime \(40 must be after 50\)`)
	c.Check(resp, gc.IsNil)

	// Case: Fetch fragments with unbounded time range.
	var fixture = buildFragmentsFixture()
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", mayProxy: true})

	// Asynchronously seed the fragment index with fixture data.
	time.AfterFunc(time.Millisecond, func() {
		res.replica.index.ReplaceRemote(buildFragmentSet(fixture))
	})
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		SignatureTTL: &oneSec,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		Fragments:     fixture,
		NextPageToken: 0,
	})

	// Case: Fetch fragments with bounded time range
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		BeginModTime: 100,
		EndModTime:   180,
		SignatureTTL: &oneSec,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Header: res.Header,
		Fragments: []pb.FragmentsResponse__Fragment{
			fixture[1],
			fixture[3],
		},
		NextPageToken: 0,
	})

	// Case: Fetch fragments with unbounded EndModTime
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		BeginModTime: 100,
		SignatureTTL: &oneSec,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		Fragments:     fixture[1:],
		NextPageToken: 0,
	})

	// Case: Fetch paginated fragments
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		PageLimit:    3,
		SignatureTTL: &oneSec,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		Fragments:     fixture[:3],
		NextPageToken: fixture[3].Spec.Begin,
	})
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		PageLimit:     3,
		SignatureTTL:  &oneSec,
		NextPageToken: resp.NextPageToken,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		Fragments:     fixture[3:],
		NextPageToken: 0,
	})

	// Case: Fetch with a NextPageToken which does not correspond to a
	// Begin in the fragment set.
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		SignatureTTL:  &oneSec,
		NextPageToken: 120,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		Fragments:     fixture[3:],
		NextPageToken: 0,
	})

	// Case: Fetch fragments outside of time range.
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		BeginModTime: 10000,
		EndModTime:   20000,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		NextPageToken: 0,
	})

	// Case: Fetch fragments with a NextPageToken larger than max fragment offset.
	resp, err = rjc.ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		NextPageToken: 1000,
	})
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        res.Header,
		NextPageToken: 0,
	})
}

// return a fixture which can be modified as needed over the course of a test.
var buildFragmentsFixture = func() []pb.FragmentsResponse__Fragment {
	return []pb.FragmentsResponse__Fragment{
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              40,
				ModTime:          90,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            40,
				End:              110,
				ModTime:          101,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            99,
				End:              130,
				ModTime:          200,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            131,
				End:              318,
				ModTime:          150,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            319,
				End:              400,
				ModTime:          290,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal: "a/journal",
				Begin:   380,
				End:     600,
			},
		},
	}
}

func buildFragmentSet(fragments []pb.FragmentsResponse__Fragment) fragment.CoverSet {
	var set = fragment.CoverSet{}
	for _, f := range fragments {
		set, _ = set.Add(fragment.Fragment{Fragment: f.Spec})
	}
	return set
}

var _ = gc.Suite(&FragmentsSuite{})
