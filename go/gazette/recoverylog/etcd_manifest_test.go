package recoverylog

import (
	"testing"

	gc "github.com/go-check/check"

	"github.com/pippio/api-server/discovery"
)

var (
	manifestFixtures = []FileRecord{
		{
			Id:          "id1",
			FirstOffset: 123,
			Links: map[string]struct{}{
				"foo/foo": {},
				"bar/bar": {},
			},
		},
		{
			Id:          "id2",
			FirstOffset: 456,
			Links: map[string]struct{}{
				"baz": {},
			},
		},
	}
)

type EtcdManifestSuite struct {
	etcd *discovery.EtcdMemoryService
}

func (s *EtcdManifestSuite) SetUpTest(c *gc.C) {
	s.etcd = discovery.NewEtcdMemoryService()

	s.etcd.MakeDirectory("/manifest/path")
	for _, fixture := range manifestFixtures {
		s.etcd.Announce("/manifest/path/"+fixture.Id, fixture, 0)
	}
}

func (s *EtcdManifestSuite) TestInitialFixtureLoad(c *gc.C) {
	_, err := NewEtcdManifest("/does/not/exist", s.etcd)
	c.Check(err, gc.NotNil)

	m, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Check(err, gc.IsNil)

	c.Check(m.etcdPath, gc.Equals, "/manifest/path")
	c.Check(m.index, gc.DeepEquals, map[string]*FileRecord{
		"foo/foo": &manifestFixtures[0],
		"bar/bar": &manifestFixtures[0],
		"baz":     &manifestFixtures[1],
	})
}

func (s *EtcdManifestSuite) TestFileCreation(c *gc.C) {
	m, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Assert(err, gc.IsNil)

	// File already exists.
	_, err = m.CreateFile("foo/foo", 8675309)
	c.Check(err, gc.ErrorMatches, "create of known file")

	record, err := m.CreateFile("new/file", 8675309)
	c.Check(err, gc.IsNil)
	c.Check(record.FirstOffset, gc.Equals, int64(8675309))
	c.Check(record.Links, gc.DeepEquals, map[string]struct{}{"new/file": {}})

	// Verify record was stored in etcd.
	recovered, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Check(recovered.index["new/file"], gc.DeepEquals, record)
}

func (s *EtcdManifestSuite) TestFileDeletion(c *gc.C) {
	m, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Assert(err, gc.IsNil)

	// File doesn't exist exists.
	_, err = m.DeleteFile("does/not/exist")
	c.Check(err, gc.ErrorMatches, "delete of unknown file")

	record1, err := m.DeleteFile("foo/foo")
	c.Check(err, gc.IsNil)
	c.Check(record1.Links, gc.DeepEquals, map[string]struct{}{"bar/bar": {}})

	record2, err := m.DeleteFile("baz")
	c.Check(err, gc.IsNil)
	c.Check(record2.Links, gc.DeepEquals, map[string]struct{}{})

	// Expect record |id1| was updated, and |id2| was deleted.
	recovered, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Check(recovered.index, gc.HasLen, 1)
	c.Check(recovered.index["bar/bar"], gc.DeepEquals, record1)
}

func (s *EtcdManifestSuite) TestFileRename(c *gc.C) {
	m, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Assert(err, gc.IsNil)

	_, err = m.RenameFile("does/not/exist", "foobar")
	c.Check(err, gc.ErrorMatches, "rename of unknown file")
	_, err = m.RenameFile("foo/foo", "bar/bar")
	c.Check(err, gc.ErrorMatches, "target file exists")

	record, err := m.RenameFile("foo/foo", "bing")
	c.Check(err, gc.IsNil)
	c.Check(record.Links, gc.DeepEquals, map[string]struct{}{
		"bar/bar": {},
		"bing":    {},
	})

	// Expect record |id1| was updated, and |id2| was deleted.
	recovered, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Check(recovered.index, gc.HasLen, 3)
	c.Check(recovered.index["foo/foo"], gc.IsNil)
	c.Check(recovered.index["bing"], gc.DeepEquals, record)
}

func (s *EtcdManifestSuite) TestFileLinking(c *gc.C) {
	m, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Assert(err, gc.IsNil)

	_, err = m.LinkFile("does/not/exist", "foobar")
	c.Check(err, gc.ErrorMatches, "link of unknown file")
	_, err = m.LinkFile("foo/foo", "bar/bar")
	c.Check(err, gc.ErrorMatches, "target file exists")

	record, err := m.LinkFile("foo/foo", "bing")
	c.Check(err, gc.IsNil)
	c.Check(record.Links, gc.DeepEquals, map[string]struct{}{
		"foo/foo": {},
		"bar/bar": {},
		"bing":    {},
	})

	// Expect record |id1| was updated, and |id2| was deleted.
	recovered, err := NewEtcdManifest("/manifest/path", s.etcd)
	c.Check(recovered.index, gc.HasLen, 4)
	c.Check(recovered.index["foo/foo"], gc.DeepEquals, record)
	c.Check(recovered.index["bing"], gc.DeepEquals, record)
}

var _ = gc.Suite(&EtcdManifestSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
