package gazette

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
	"github.com/stretchr/testify/mock"
)

type WriteServiceSuite struct{}

// errReader wraps a Reader, and replaces an EOF with an error.
type errReader struct{ io.Reader }

func (r errReader) Read(p []byte) (int, error) {
	if n, err := r.Reader.Read(p); err == io.EOF {
		return n, errors.New("error!")
	} else {
		return n, err
	}
}

func (s *WriteServiceSuite) TestBasicWriteSpooling(c *gc.C) {
	iface := pendingWritePool.Get()

	c.Assert(iface, gc.FitsTypeOf, (*pendingWrite)(nil))
	write := iface.(*pendingWrite)

	// Expect |journal| isn't initialized.
	c.Check(write.journal, gc.Equals, journal.Name(""))
	write.journal = "test/journal"

	// Expect |offset| is zero.
	c.Check(write.offset, gc.Equals, int64(0))

	// Expect backing-file |write| is already removed.
	_, err := os.Stat(write.file.Name())
	c.Check(os.IsNotExist(err), gc.Equals, true)

	c.Check(writeAllOrNone(write,
		errReader{strings.NewReader("xxxxxxxx")}), gc.ErrorMatches, "error!")
	c.Check(writeAllOrNone(write, strings.NewReader("foo")), gc.IsNil)
	c.Check(writeAllOrNone(write,
		errReader{strings.NewReader("yyyyyyy")}), gc.ErrorMatches, "error!")
	c.Check(writeAllOrNone(write, strings.NewReader("bar")), gc.IsNil)
	c.Check(writeAllOrNone(write,
		errReader{strings.NewReader("zzz")}), gc.ErrorMatches, "error!")

	c.Check(write.offset, gc.Equals, int64(6))

	// Check for expected file content (only includes readers without errors).
	_, err = write.file.Seek(0, 0)
	c.Check(err, gc.IsNil)
	data, err := ioutil.ReadAll(io.LimitReader(write.file, write.offset))
	c.Check(err, gc.IsNil)
	c.Check(string(data), gc.Equals, "foobar")

	c.Check(releasePendingWrite(write), gc.IsNil)
}

func (s *WriteServiceSuite) TestParallelWriteSpooling(c *gc.C) {
	// Very basic sanity-check that raced pendingWrite uses don't blow up.
	// This is not exhaustive!
	kParallel, kLoops := 5, 100
	done := make(chan struct{}, kParallel)

	for i := 0; i != kParallel; i++ {
		go func() {
			for j := 0; j != kLoops; j++ {
				s.TestBasicWriteSpooling(c)
				runtime.Gosched()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i != kParallel; i++ {
		<-done
	}
}

func (s *WriteServiceSuite) TestWriteLifecycle(c *gc.C) {
	// Shorten the write error cool-off interval for this test.
	actualTimeout := kWriteServiceCooloffTimeout
	kWriteServiceCooloffTimeout = time.Millisecond
	defer func() { kWriteServiceCooloffTimeout = actualTimeout }()

	var mockClient mockHttpClient

	client, _ := NewClient("http://server")
	client.httpClient = &mockClient

	// Pre-fill client's route cache for /a/journal and /another/journal.
	client.locationCache.Add("/a/journal", newURL("http://server/a/journal"))
	client.locationCache.Add("/another/journal", newURL("http://server/another/journal"))

	writer := NewWriteService(client)
	writer.SetConcurrency(2) // Exercise start/stop of concurrent service loops.

	// Perform a sequence of writes, several of which have broken readers.
	_, err := writer.ReadFrom("a/journal", errReader{strings.NewReader("xxx")})
	c.Check(err, gc.ErrorMatches, "error!")

	// First successful write.
	fooPromise, err := writer.Write("a/journal", []byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(fooPromise, gc.NotNil)

	// Broken write.
	_, err = writer.ReadFrom("a/journal", errReader{strings.NewReader("yyy")})
	c.Check(err, gc.ErrorMatches, "error!")

	// Second successful write.
	barPromise, err := writer.ReadFrom("a/journal", strings.NewReader("bar"))
	c.Check(err, gc.IsNil)
	c.Check(barPromise, gc.NotNil)

	// Broken write.
	_, err = writer.ReadFrom("a/journal", errReader{strings.NewReader("zzz")})
	c.Check(err, gc.ErrorMatches, "error!")

	// Third write, to a different journal.
	bazPromise, err := writer.Write("another/journal", []byte("baz!"))
	c.Check(err, gc.IsNil)
	c.Check(bazPromise, gc.NotNil)

	// PUT to a/journal. Expect to see both successful writes, batched together.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "PUT" && request.URL.Path == "/a/journal"
	})).Return(&http.Response{
		StatusCode: http.StatusNoContent, // Success.
		Body:       ioutil.NopCloser(strings.NewReader("")),
	}, nil).Run(func(args mock.Arguments) {
		request := args[0].(*http.Request)

		c.Check(request.Method, gc.Equals, "PUT")

		content, _ := ioutil.ReadAll(request.Body)
		c.Check(string(content), gc.Equals, "foobar")
	}).Once()

	// PUT to another/journal. Fails with a remote server error.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "PUT" && request.URL.Path == "/another/journal"
	})).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "Whoops!",
		Body:       ioutil.NopCloser(strings.NewReader("")),
	}, nil).Run(func(args mock.Arguments) {
		request := args[0].(*http.Request)

		content, _ := ioutil.ReadAll(request.Body)
		c.Check(string(content), gc.Equals, "baz!")
	}).Once()

	// Expect PUT to another/journal is retried with the same content.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "PUT" && request.URL.Path == "/another/journal"
	})).Return(&http.Response{
		StatusCode: http.StatusNoContent, // Success.
		Body:       ioutil.NopCloser(strings.NewReader("")),
	}, nil).Run(func(args mock.Arguments) {
		request := args[0].(*http.Request)

		content, _ := ioutil.ReadAll(request.Body)
		c.Check(string(content), gc.Equals, "baz!")
	}).Once()

	writer.Start()

	// Expect that promises have been resolved.
	<-fooPromise.Ready
	<-barPromise.Ready
	<-bazPromise.Ready

	writer.Stop()
	// Expect that after Stop(), all writes have flushed.
	mockClient.AssertExpectations(c)
}

var _ = gc.Suite(&WriteServiceSuite{})
