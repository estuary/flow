package cloudstore

// How to test individual cloudstore.Filesystem implementations:
//  * Local: go test -v github.com/pippio/cloudstore -check.vv -cloudFS file:///tmp/path
//  * GCS:   go test -v github.com/pippio/cloudstore -check.vv -cloudFS "gs://pippio-uploads/test?compress" -gcpServiceAccount /path/to/account/credentials.json

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"

	"github.com/pippio/consensus"
	"github.com/pippio/endpoints"
)

const (
	quote = `Great things are done by a series of small things
		brought together. -Van Gogh`

	megabyte = 1024 * 1024
)

var (
	s3AccessKeyID     = flag.String("testS3AccessKeyID", "", "S3 Access Key ID")
	s3SecretAccessKey = flag.String("testS3SecretAccessKey", "", "S3 Secret Access Key")
	s3Region          = flag.String("testS3Region", "us-east-1", "S3 Region")
)

type FileSystemSuite struct {
	cfs FileSystem
}

func (s *FileSystemSuite) SetUpSuite(c *gc.C) {
	var fsProperties Properties

	endpoints.ParseFromEnvironment()
	s3ParseFromEnvironment(c)
	flag.Parse()

	// Prepare random seed for large file test.
	rand.Seed(time.Now().Unix())

	if *s3AccessKeyID != "" {
		var fakeProperties = MapProperties{
			AWSAccessKeyID:     *s3AccessKeyID,
			AWSSecretAccessKey: *s3SecretAccessKey,
			S3Region:           *s3Region,
		}

		fsProperties = fakeProperties
	} else {
		// If a real CFS can be prepared, use it for this test.
		etcdClient, err := etcd.New(etcd.Config{
			Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
		if err != nil {
			c.Log("Using temp filesystem: no connectivity to etcd: ", err)
			s.cfs = NewTmpFileSystem()
			return
		}
		keysAPI := etcd.NewKeysAPI(etcdClient)

		properties, err := keysAPI.Get(context.Background(), "/properties",
			&etcd.GetOptions{Recursive: true, Sort: true})
		if err != nil {
			c.Log("Using temp filesystem: failed to initialize /properties: ", err)
			s.cfs = NewTmpFileSystem()
			return
		}

		fsProperties = consensus.MapAdapter(properties.Node)
	}

	var err error
	s.cfs, err = DefaultFileSystem(fsProperties)
	if err != nil {
		c.Log("Using temp filesystem: failed to initialize DefaultFilesystem: ", err)
		s.cfs = NewTmpFileSystem()
	}
}

func (s *FileSystemSuite) SetUpTest(c *gc.C) {
	// Write a basic file fixture for examination by tests.
	c.Assert(s.cfs.MkdirAll("path/to/", 0750), gc.IsNil)
	file, err := s.cfs.OpenFile("path/to/fixture",
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	c.Assert(err, gc.IsNil)

	file.Write([]byte(quote[:5]))
	file.Write([]byte(quote[5:6]))
	file.Write([]byte(quote[6:]))
	c.Check(file.Close(), gc.IsNil)
}

func (s *FileSystemSuite) TearDownSuite(c *gc.C) {
	s.cfs.Remove("path/to/fixture")
	s.cfs.Close()
}

func (s *FileSystemSuite) TestStatFile(c *gc.C) {
	file, err := s.cfs.Open("path/to/fixture")
	c.Assert(err, gc.IsNil)

	info, err := file.Stat()
	c.Assert(err, gc.IsNil)

	// Assert that each implementation can work as an |os.FileInfo|.
	osi, ok := info.(os.FileInfo)
	c.Assert(ok, gc.Equals, true)

	c.Check(osi.IsDir(), gc.Equals, false)
	c.Check(osi.ModTime().After(time.Time{}), gc.Equals, true) // Non-nil.
	c.Check(osi.Name(), gc.Equals, "fixture")
	c.Check(osi.Size() > 0, gc.Equals, true) // May fluctuate if compressed.
}

func (s *FileSystemSuite) TestStatDir(c *gc.C) {
	for _, testCase := range []struct{ path, name string }{
		{"path/", "path"},
		{"path", "path"},
		{"path/to/", "to"},
		{"path/to", "to"},
	} {
		dir, err := s.cfs.Open(testCase.path)
		c.Assert(err, gc.IsNil)

		info, err := dir.Stat()
		c.Assert(err, gc.IsNil)

		c.Check(info.IsDir(), gc.Equals, true)
		c.Check(info.Name(), gc.Equals, testCase.name)
	}
	// Also test handling of stat-ing the filesystem root.
	dir, err := s.cfs.Open("")
	c.Assert(err, gc.IsNil)

	info, err := dir.Stat()
	c.Assert(err, gc.IsNil)

	c.Check(info.IsDir(), gc.Equals, true)
	// We don't know what the name should be in the context of this tests (since
	// it's the name of the root directory or bucket), but expect it to not be
	// the empty string.
	c.Check(info.Name(), gc.Not(gc.Equals), "")
}

func (s *FileSystemSuite) TestReaddirBasic(c *gc.C) {
	dir, err := s.cfs.Open("")
	c.Assert(err, gc.IsNil)

	results, err := dir.Readdir(-1)
	c.Check(err, gc.IsNil)

	// Expect a "path" directory to be present. Additional directories may
	// also be present, if we're running on a developer machine.
	c.Check(len(results) > 0, gc.Equals, true)

	var found bool
	for _, result := range results {
		if result.Name() == "path" {
			c.Check(results[0].IsDir(), gc.Equals, true)
			found = true
			break
		}
	}
	if !found {
		c.Fail()
	}

	dir, err = s.cfs.Open("path")
	c.Assert(err, gc.IsNil)

	results, err = dir.Readdir(10)
	c.Check(err, gc.IsNil)

	c.Check(results, gc.HasLen, 1)
	c.Check(results[0].IsDir(), gc.Equals, true)
	c.Check(results[0].Name(), gc.Equals, "to")

	dir, err = s.cfs.Open("path/to/")
	c.Assert(err, gc.IsNil)

	results, err = dir.Readdir(10)
	c.Check(err, gc.IsNil)

	c.Check(results, gc.HasLen, 1)
	c.Check(results[0].IsDir(), gc.Equals, false)
	c.Check(results[0].Name(), gc.Equals, "fixture")

	results, err = dir.Readdir(10)
	c.Check(results, gc.HasLen, 0)
	c.Check(err, gc.Equals, io.EOF)
}

func (s *FileSystemSuite) TestReaddirMultipleCalls(c *gc.C) {
	var numFilesInDir = 20

	// Induce multiple calls to ListObjectsV2 for S3 driver.
	s3MaxListObjectsKeys = boxInt64(int64(numFilesInDir) / 2)

	// Prepare cleanup that should run on any panic or failure.
	defer func() {
		// Restore object listing behavior.
		s3MaxListObjectsKeys = nil

		for i := 0; i != numFilesInDir; i++ {
			var name = fmt.Sprintf("path/to/file-%02d", i)
			var err = s.cfs.Remove(name)
			if err != nil && err != os.ErrNotExist {
				c.Log("could not cleanup: ", name, " due to: ", err)
				c.Fail()
			}
		}
	}()

	// Create a number of expected files in the test directory.
	expect := map[string]struct{}{"fixture": struct{}{}}
	for i := 0; i != numFilesInDir; i++ {
		name := fmt.Sprintf("file-%02d", i)
		expect[name] = struct{}{}

		file, _ := s.cfs.OpenFile("path/to/"+name,
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
		c.Check(file.Close(), gc.IsNil)
	}
	// Enumerate the directory, marking files seen.
	dir, err := s.cfs.Open("path/to/")
	c.Assert(err, gc.IsNil)

	// Perform repeated calls to Readdir() with differing counts and results.
	for _, iter := range []struct{ ask, actual int }{{10, 10}, {9, 9}, {8, 2}} {
		results, err := dir.Readdir(iter.ask)

		c.Check(err, gc.IsNil)
		c.Check(results, gc.HasLen, iter.actual)

		for _, result := range results {
			if _, ok := expect[result.Name()]; !ok {
				c.Log("unexpected result: ", result)
				c.Fail()
			}
			delete(expect, result.Name())
		}
	}
	c.Check(expect, gc.HasLen, 0)
}

func (s *FileSystemSuite) TestReadFile(c *gc.C) {
	file, err := s.cfs.Open("path/to/fixture")
	c.Assert(err, gc.IsNil)
	c.Check(file, gc.NotNil)

	content, err := ioutil.ReadAll(file)
	c.Check(err, gc.IsNil)
	c.Check(string(content), gc.Equals, quote)

	c.Check(file.Close(), gc.IsNil)
}

func (s *FileSystemSuite) TestRemoveFile(c *gc.C) {
	c.Check(s.cfs.Remove("path/to/fixture"), gc.IsNil)

	file, err := s.cfs.Open("path/to/fixture")
	c.Check(file, gc.IsNil)
	c.Check(err, gc.NotNil)
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *FileSystemSuite) TestUpdateFile(c *gc.C) {
	{
		w, err := s.cfs.OpenFile("path/to/fixture", os.O_WRONLY|os.O_TRUNC, 0640)
		c.Assert(err, gc.IsNil)

		w.Write([]byte("updated content"))
		c.Check(w.Close(), gc.IsNil)
	}
	{
		r, err := s.cfs.Open("path/to/fixture")
		c.Check(err, gc.IsNil)

		content, err := ioutil.ReadAll(r)
		c.Check(err, gc.IsNil)
		c.Check(string(content), gc.Equals, "updated content")
	}
}

func (s *FileSystemSuite) TestCreateExclusiveEmptyFile(c *gc.C) {
	defer s.cfs.Remove("path/to/empty-file")

	{
		file, err := s.cfs.OpenFile("path/to/empty-file",
			os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
		c.Assert(err, gc.IsNil)
		c.Check(file, gc.NotNil)
		c.Check(file.Close(), gc.IsNil)
	}
	{
		_, err := s.cfs.Open("path/to/empty-file")
		c.Check(err, gc.IsNil) // Visible.
	}
}

func (s *FileSystemSuite) TestOpenReadNotExist(c *gc.C) {
	file, err := s.cfs.Open("path/does/not/exist")
	c.Check(file, gc.IsNil)
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *FileSystemSuite) TestOpenWriteNotExist(c *gc.C) {
	file, err := s.cfs.OpenFile("path/does/not/exist",
		os.O_WRONLY|os.O_TRUNC, 0640)
	c.Check(file, gc.IsNil)
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *FileSystemSuite) TestCreateExclusiveExists(c *gc.C) {
	file, err := s.cfs.OpenFile("path/to/fixture",
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	c.Check(file, gc.IsNil)
	c.Check(os.IsExist(err), gc.Equals, true)
}

func (s *FileSystemSuite) TestRemoveNotExist(c *gc.C) {
	err := s.cfs.Remove("path/does/not/exist")
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *FileSystemSuite) TestCopyAtomicSuccess(c *gc.C) {
	{
		w, err := s.cfs.OpenFile("path/to/transfer",
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
		c.Assert(err, gc.IsNil)

		r := strings.NewReader("transferred content")

		n, err := s.cfs.CopyAtomic(w, r)
		c.Check(n, gc.Equals, int64(19))
		c.Check(err, gc.IsNil)
	}
	{
		r, err := s.cfs.Open("path/to/transfer")
		c.Assert(err, gc.IsNil)

		content, err := ioutil.ReadAll(r)
		c.Check(err, gc.IsNil)
		c.Check(string(content), gc.Equals, "transferred content")
	}
	c.Check(s.cfs.Remove("path/to/transfer"), gc.IsNil) // Cleanup.
}

func (s *FileSystemSuite) TestPartialCopy(c *gc.C) {
	{
		w, err := s.cfs.OpenFile("path/to/partial-transfer",
			os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
		c.Assert(err, gc.IsNil)

		r := iotest.TimeoutReader(iotest.OneByteReader(
			strings.NewReader("transferred content")))

		n, err := s.cfs.CopyAtomic(w, r)
		c.Check(n, gc.Equals, int64(1))
		c.Check(err, gc.Equals, iotest.ErrTimeout)
	}
	{
		_, err := s.cfs.Open("path/to/partial-transfer")
		c.Check(err, gc.NotNil)
		c.Check(os.IsNotExist(err), gc.Equals, true)
	}
}

func (s *FileSystemSuite) TestSeek(c *gc.C) {
	var buffer [len(quote)]byte

	file, err := s.cfs.Open("path/to/fixture")
	c.Assert(err, gc.IsNil)

	// Seek forward to offset 10.
	n, err := file.Seek(10, 0)
	c.Check(n, gc.Equals, int64(10))
	c.Check(err, gc.IsNil)

	_, err = file.Read(buffer[10:15])
	c.Check(err, gc.IsNil)
	c.Check(string(buffer[10:15]), gc.Equals, quote[10:15])

	// Seek backwards by 10 (to offset 5).
	n, err = file.Seek(-10, 1)
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, int64(5))

	_, err = file.Read(buffer[5:10])
	c.Check(err, gc.IsNil)
	c.Check(string(buffer[5:15]), gc.Equals, quote[5:15])

	// Seek to 0 (absolute).
	n, err = file.Seek(0, 0)
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, int64(0))

	_, err = file.Read(buffer[0:5])
	c.Check(err, gc.IsNil)
	c.Check(string(buffer[:15]), gc.Equals, quote[:15])
}

func (s *FileSystemSuite) TestToURL(c *gc.C) {
	url, err := s.cfs.ToURL("path/to/fixture", "GET", time.Minute)
	c.Check(err, gc.IsNil)

	// Build an http transport which can also serve from the file local system.
	transport := &http.Transport{}
	transport.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	client := &http.Client{Transport: transport}

	c.Log(url.String())

	if s.cfs.ProducesAuthorizedURL() {
		response, err := client.Get(url.String())
		c.Check(err, gc.IsNil)

		body, err := ioutil.ReadAll(response.Body)
		c.Check(err, gc.IsNil)
		c.Check(string(body), gc.Equals, quote)
	}
}

func (s *FileSystemSuite) TestHugeFileRoundtrip(c *gc.C) {
	if testing.Short() {
		c.Skip("huge file test does not run in short mode")
		return
	}

	// Write a temporary huge file. Use S3 max spool size as a guideline,
	// so we are sure to flush at least two spools. And add a megabyte so that
	// we don't finish writing on a spool boundary.
	tempFile, err := ioutil.TempFile("", "cfsHugeFile")
	c.Assert(err, gc.IsNil)
	defer os.Remove(tempFile.Name())

	// Concurrently write to cloud storage.
	defer s.cfs.Remove("path/to/huge-file")
	wrFile, err := s.cfs.OpenFile("path/to/huge-file",
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)

	var writer = io.MultiWriter(wrFile, tempFile)
	var buf [megabyte]byte
	var targetSize = megabyte + 2*MaxSpoolSizeBytes
	for b := 0; b < targetSize; b += len(buf) {
		rand.Read(buf[:])
		_, err := writer.Write(buf[:])
		c.Assert(err, gc.IsNil)
	}

	// Reopen file for reading.
	wrFile.Close()
	rdFile, err := s.cfs.Open("path/to/huge-file")
	c.Assert(err, gc.IsNil)

	// Rewind temp file.
	tempFile.Seek(0, 0)

	var buf2 [megabyte]byte
	for b := 0; b < targetSize; b += len(buf) {
		n, err := io.ReadFull(tempFile, buf[:])
		c.Assert(err, gc.IsNil)
		c.Check(n, gc.Equals, len(buf))

		n, err = io.ReadFull(rdFile, buf2[:])
		c.Assert(err, gc.IsNil)
		c.Check(n, gc.Equals, len(buf2))
		c.Check(bytes.Compare(buf[:], buf2[:]), gc.Equals, 0)
	}
}

func (s *FileSystemSuite) TestWalk(c *gc.C) {
	var test = func(prefix string) {
		var accum []string
		s.cfs.Walk(prefix, func(n string, _ os.FileInfo, _ error) error {
			accum = append(accum, n)
			return nil
		})
		c.Check(accum, gc.HasLen, 1)
		c.Check(accum[0], gc.Equals, "path/to/fixture")
	}

	// This won't work in the case where -cloudFS does not specify a bucket.
	// test("")

	// All should return the same result.
	test("path")
	test("path/to")
}

func boxInt64(n int64) *int64 { return &n }

func s3ParseFromEnvironment(c *gc.C) {
	var val = os.Getenv("AWS_ACCESS_KEY_ID")
	if val != "" {
		c.Log("got AWS_ACCESS_KEY_ID environment variable")
		*s3AccessKeyID = val
	}

	val = os.Getenv("AWS_SECRET_ACCESS_KEY")
	if val != "" {
		c.Log("got AWS_SECRET_ACCESS_KEY environment variable")
		*s3SecretAccessKey = val
	}
}

var _ = gc.Suite(&FileSystemSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
