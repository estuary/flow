package shuffle

/*
import (
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestJournalGrouping(t *testing.T) {
	var fixture pb.ListResponse
	for _, j := range []string{
		"foo/bar=1/baz=abc/part=00",
		"foo/bar=1/baz=abc/part=01",
		"foo/bar=1/baz=def/part=00",
		"foo/bar=2/baz=def/part=00",
		"foo/bar=2/baz=def/part=01",
	} {
		fixture.Journals = append(fixture.Journals, pb.ListResponse_Journal{
			Spec: pb.JournalSpec{Name: pb.Journal(j)},
		})
	}

	assert.Equal(t, [][]pb.ListResponse_Journal{
		fixture.Journals[:2],
		fixture.Journals[2:3],
		fixture.Journals[3:5],
	}, GroupCommonDirs(&fixture))
}
*/
