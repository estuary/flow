package fragment

import (
	"context"
	"flag"
	"io/ioutil"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/codecs"
	pb "go.gazette.dev/core/broker/protocol"
)

// How to test individual FragmentStore implementations:
//
// S3:
// 1) Set minio credentials & config to AWS application default credentials (~/.aws):
// $ cp kustomize/test/bases/environment/minio_credentials ~/.aws/credentials
// $ cp kustomize/test/bases/environment/minio_config ~/.aws/config
//
// 2) Test, enabling SDK config loading, and replacing Minio endpoint with correct service IP. Eg:
// $ AWS_SDK_LOAD_CONFIG=1 go test -v ./broker/fragment -test.run TestStoreInteractions \
//   -store "s3://examples/frag-test/?find=rwFind&replace=rwReplace&profile=minio&endpoint=http%3A%2F%2F10.152.183.246%3A9000"
//
// GCS:
// 1) Write credentials under ~/.config/gcloud/application_default_credentials.json
//
// 2) Test:
// $ go test -v ./broker/fragment -test.run TestStoreInteractions \
//   -store "gs://bucket-that-i-own/frag-test/?find=rwFind&replace=rwReplace"

var storeEndpoint = flag.String("store",
	"file:///?find=rwFind&replace=rwReplace",
	"store endpoint under test")

func TestStoreInteractions(t *testing.T) {
	var fs = pb.FragmentStore(*storeEndpoint)
	require.NoError(t, fs.Validate())

	var dir, err = ioutil.TempDir("", "stores_test")
	require.NoError(t, err)

	defer client.InstallFileTransport(dir)()
	defer os.RemoveAll(dir)

	defer func(s string) { FileSystemStoreRoot = s }(FileSystemStoreRoot)
	FileSystemStoreRoot = dir

	// Use a wacky path template which does little more than ensure
	// that each fragment ends up under a different path partition.
	var spec = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores:              []pb.FragmentStore{fs},
			PathPostfixTemplate: `nanos={{ .Spool.FirstAppendTime.Nanosecond }}`,
		},
	}
	// Begin by persisting a number of fragment fixtures.
	// Persist twice, to exercise handling where the fragment exists.
	var ctx = context.Background()
	for _, spool := range buildSpoolFixtures(t) {
		require.NoError(t, Persist(ctx, spool, spec))
		require.NoError(t, Persist(ctx, spool, spec))
	}

	// List fragments of both journals.
	var fooFrags []pb.Fragment
	require.NoError(t, List(ctx, fs, tstRWFoo,
		func(f pb.Fragment) { fooFrags = append(fooFrags, f) }))

	var barFrags []pb.Fragment
	require.NoError(t, List(ctx, fs, tstBar,
		func(f pb.Fragment) { barFrags = append(barFrags, f) }))

	require.Len(t, fooFrags, 3)
	require.Len(t, barFrags, 2)

	// The PathPostfixTemplate can return fragments which aren't ordered by offset.
	for _, frags := range [][]pb.Fragment{fooFrags, barFrags} {
		sort.Slice(frags, func(i, j int) bool { return frags[i].Begin < frags[j].Begin })
	}

	// Expect we can also list fragments of tstRWFoo directly, post-rewrite.
	var fooFragsPostRW []pb.Fragment
	require.NoError(t, List(ctx, fs, "tst/rwReplace/foo",
		func(f pb.Fragment) { fooFragsPostRW = append(fooFragsPostRW, f) }))

	require.Len(t, fooFragsPostRW, 3)

	// Fragments reflect their correct listed journals.
	require.Equal(t, tstRWFoo, fooFrags[0].Journal)
	require.Equal(t, tstBar, barFrags[0].Journal)
	require.Equal(t, pb.Journal("tst/rwReplace/foo"), fooFragsPostRW[0].Journal)

	// We can open and read expected fragment content.
	require.Equal(t, tstRWFooData[1], readFrag(t, fooFrags[1]), fooFrags)
	require.Equal(t, tstBarData[0], readFrag(t, barFrags[0]), barFrags)

	// We can create a signed URL and GET it.
	getURL, err := SignGetURL(fooFrags[0], time.Minute)
	require.NoError(t, err)
	fr, err := client.OpenFragmentURL(ctx, fooFrags[0], 0, getURL)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	require.Equal(t, tstRWFooData[0], string(b))

	// We can remove all fragments.
	for _, f := range fooFrags {
		require.NoError(t, Remove(ctx, f))
	}
	for _, f := range barFrags {
		require.NoError(t, Remove(ctx, f))
	}

	// We no longer see them in listings.
	require.NoError(t, List(ctx, fs, tstRWFoo,
		func(f pb.Fragment) { panic("no fragments, not called") }))
	require.NoError(t, List(ctx, fs, tstBar,
		func(f pb.Fragment) { panic("not called") }))
}

func TestParseStoreArgsS3(t *testing.T) {
	storeURL, _ := url.Parse("s3://bucket/prefix/?endpoint=https://s3.region.amazonaws.com&SSE=kms&SSEKMSKeyId=123")
	var s3Cfg S3StoreConfig
	parseStoreArgs(storeURL, &s3Cfg)
	require.Equal(t, "bucket", storeURL.Host)
	require.Equal(t, "prefix/", storeURL.Path[1:])
	require.Equal(t, "https://s3.region.amazonaws.com", s3Cfg.Endpoint)
	require.Equal(t, "kms", s3Cfg.SSE)
	require.Equal(t, "123", s3Cfg.SSEKMSKeyId)
}

func readFrag(t *testing.T, f pb.Fragment) string {
	var rc, err = Open(context.Background(), f)
	require.NoError(t, err)
	dec, err := codecs.NewCodecReader(rc, f.CompressionCodec)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(dec)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	return string(b)
}

func buildSpoolFixtures(t *testing.T) []Spool {
	var runSeq = func(s *Spool, vals []string, primary bool) {
		for _, v := range vals {
			require.NoError(t, s.applyContent(&pb.ReplicateRequest{
				Content: []byte(v),
			}))

			// Commit.
			var p = s.Next()
			require.Equal(t, pb.Status_OK, s.applyCommit(&pb.ReplicateRequest{
				Proposal:  &p,
				Registers: new(pb.LabelSet),
			}, primary).Status)

			// Roll spool.
			p.Begin = p.End
			p.Sum = pb.SHA1Sum{}

			require.Equal(t, pb.Status_OK, s.applyCommit(&pb.ReplicateRequest{
				Proposal:  &p,
				Registers: new(pb.LabelSet),
			}, primary).Status)

		}
	}
	var (
		obv testSpoolObserver
		s1  = NewSpool(tstRWFoo, &obv)
		s2  = NewSpool(tstBar, &obv)
	)
	s1.CompressionCodec = pb.CompressionCodec_SNAPPY
	s2.CompressionCodec = pb.CompressionCodec_GZIP

	runSeq(&s1, tstRWFooData, true)
	runSeq(&s2, tstBarData, false)

	require.Len(t, obv.completes, len(tstRWFooData)+len(tstBarData))
	return obv.completes
}

var (
	tstRWFoo     pb.Journal = "tst/rwFind/foo"
	tstRWFooData            = []string{
		`Great things are done by a series of small things
			brought together. -Van Gogh`,
		"and other",
		"data",
	}

	tstBar     pb.Journal = "tst/bar"
	tstBarData            = []string{
		`I am a part of all that I have met;
			Yet all experience is an arch wherethro'
			Gleams that untravell'd world whose margin fades
			For ever and forever when I move. -Tennyson`,
		"second commit",
	}
)
