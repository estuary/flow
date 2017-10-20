package gazette

import (
	"bytes"
	"errors"
	"expvar"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/LiveRamp/gazette/journal"
)

const (
	kFragmentFixtureStr = "00000000000003e8-00000000000007d0-" +
		"0102030405060708090a0b0c0d0e0f1011121314"
	kFragmentLastModifiedStr = "Mon, 11 Jul 2016 23:45:59 GMT"
)

var (
	fakeSum = [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	fragmentFixture = journal.Fragment{
		Journal:       "a/journal",
		Begin:         1000,
		End:           2000,
		RemoteModTime: time.Date(2016, 7, 11, 23, 45, 59, 0, time.UTC),
		Sum:           fakeSum,
	}
)

type ClientSuite struct {
	client *Client
}

func (s *ClientSuite) SetUpTest(c *gc.C) {
	// Clear the normally global stats-map for each test.
	gazetteMap.Init()

	client, err := NewClient("http://default")
	c.Assert(err, gc.IsNil)
	client.timeNow = func() time.Time { return time.Unix(1234, 0) } // Fix time.
	s.client = client
}

func newReadResponseFixture() *http.Response {
	return &http.Response{
		// Return a successful HEAD response, which was redirected from http://default.
		StatusCode: http.StatusPartialContent,
		Header: http.Header{
			"Content-Range":            []string{"bytes 1005-9999999999/9999999999"},
			WriteHeadHeader:            []string{"3000"},
			FragmentNameHeader:         []string{kFragmentFixtureStr},
			FragmentLastModifiedHeader: []string{kFragmentLastModifiedStr},
			FragmentLocationHeader:     []string{"http://cloud/fragment/location"},
		},
		Request: &http.Request{
			URL: newURL("http://redirected-server/a/journal"),
		},
		Body: ioutil.NopCloser(strings.NewReader("body")),
	}
}

func (s *ClientSuite) TestHeadRequestWithInvalidation(c *gc.C) {
	var mockClient = &mockHttpClient{}

	// Note we use a slightly non-standard fixture of "a/journal/path"
	// (vs "a/journal") to excercise handling of URL.Path re-writes
	// in this test.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.String() == "http://default/a/journal/path?block=true&offset=1005"
	})).Return(newReadResponseFixture(), nil).Once()

	s.client.httpClient = mockClient
	result, loc := s.client.Head(
		journal.ReadArgs{Journal: "a/journal/path", Offset: 1005, Blocking: true})

	var expectFragment = fragmentFixture
	expectFragment.Journal = "a/journal/path"

	c.Check(result, gc.DeepEquals, journal.ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  expectFragment,
	})
	c.Check(loc, gc.DeepEquals, newURL("http://cloud/fragment/location"))

	mockClient.AssertExpectations(c)

	// Expect that the redirected location was cached.
	cached, _ := s.client.locationCache.Get("/a/journal/path")
	c.Check(cached, gc.DeepEquals, newURL("http://redirected-server/a/journal"))

	// Repeat the request. This time, the cached server returns a network error.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.String() == "http://redirected-server/a/journal?block=false&offset=1005"
	})).Return(nil, io.ErrUnexpectedEOF).Once()

	result, loc = s.client.Head(
		journal.ReadArgs{Journal: "a/journal/path", Offset: 1005, Blocking: false})

	c.Check(result.Error, gc.Equals, io.ErrUnexpectedEOF)
	c.Check(loc, gc.IsNil)

	// Expect the cache was cleared.
	_, ok := s.client.locationCache.Get("/a/journal/path")
	c.Check(ok, gc.Equals, false)
}

func (s *ClientSuite) TestDirectGet(c *gc.C) {
	mockClient := &mockHttpClient{}

	responseFixture := newReadResponseFixture()
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "GET" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(responseFixture, nil).Once()

	s.client.httpClient = mockClient
	result, body := s.client.GetDirect(journal.ReadArgs{
		Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result, gc.DeepEquals, journal.ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	mockClient.AssertExpectations(c)

	// Expect server's response body is plugged into the stats-wrapper.
	c.Check(body.(readStatsWrapper).stream, gc.Equals, responseFixture.Body)
}

func (s *ClientSuite) TestDirectGetFails(c *gc.C) {
	mockClient := &mockHttpClient{}

	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "GET" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "Internal Error",
		Body:       ioutil.NopCloser(strings.NewReader("message")),
	}, nil).Once()

	s.client.httpClient = mockClient
	result, body := s.client.GetDirect(journal.ReadArgs{
		Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result.Error.Error(), gc.Equals, "Internal Error (message)")
	c.Check(body, gc.IsNil)

	mockClient.AssertExpectations(c)
}

func (s *ClientSuite) TestGetWithoutFragmentLocation(c *gc.C) {
	mockClient := &mockHttpClient{}

	responseFixture := newReadResponseFixture()
	responseFixture.Header.Del(FragmentLocationHeader)

	// Expect an initial non-blocking HEAD request to the default endpoint.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(responseFixture, nil).Once()

	// Expect a direct blocking GET request to the previously-redirected endpoint.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "GET" &&
			request.URL.String() == "http://redirected-server/a/journal?block=true&blockms=6000&offset=1005"
	})).Return(responseFixture, nil).Once()

	// Arbitrary offset fixture.
	s.client.httpClient = mockClient
	result, body := s.client.Get(journal.ReadArgs{
		Journal: "a/journal", Offset: 1005, Blocking: true, Deadline: time.Unix(1240, 0)})

	c.Check(result, gc.DeepEquals, journal.ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	mockClient.AssertExpectations(c)

	// Expect server's response body is plugged into the stats-wrapper.
	c.Check(body.(readStatsWrapper).stream, gc.Equals, responseFixture.Body)

	// Before the read, head is at the requested offset.
	readerMap := gazetteMap.Get("readers").(*expvar.Map).Get("a/journal").(*expvar.Map)
	c.Check(readerMap.Get("head").(*expvar.Int).String(), gc.Equals, "1005")

	// Read the data out, so our stats-wrapper sees it ("body" -- 4 bytes)
	io.Copy(ioutil.Discard, body)

	// After the read, byte counter goes up and offset advances with the size
	// of the read.
	c.Check(readerMap.Get("bytes").(*expvar.Int).String(), gc.Equals, "4")
	c.Check(readerMap.Get("head").(*expvar.Int).String(), gc.Equals, "1009")
}

func (s *ClientSuite) TestGetWithFragmentLocation(c *gc.C) {
	mockClient := &mockHttpClient{}

	// Expect an initial HEAD request to the default endpoint.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(newReadResponseFixture(), nil).Once()

	// Expect a following GET request to the returned cloud URL.
	mockClient.On("Get", "http://cloud/fragment/location").Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("xxxxxfragment-content...")),
	}, nil).Once()

	s.client.httpClient = mockClient
	result, body := s.client.Get(
		journal.ReadArgs{Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result.Error, gc.IsNil)
	c.Check(result, gc.DeepEquals, journal.ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	mockClient.AssertExpectations(c)

	// Expect that the returned response is pre-seeked to the correct offset.
	data, _ := ioutil.ReadAll(body)
	c.Check(string(data), gc.Equals, "fragment-content...")
}

func (s *ClientSuite) TestGetWithFragmentLocationFails(c *gc.C) {
	mockClient := &mockHttpClient{}

	// Expect an initial HEAD request to the default endpoint.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(newReadResponseFixture(), nil).Once()

	// Expect a following GET request to the returned cloud URL, which fails.
	mockClient.On("Get", "http://cloud/fragment/location").Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "Internal Error",
		Body:       ioutil.NopCloser(strings.NewReader("message")),
	}, nil).Once()

	s.client.httpClient = mockClient
	result, body := s.client.Get(
		journal.ReadArgs{Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result.Error.Error(), gc.Equals, "fetching fragment: Internal Error")
	c.Check(body, gc.IsNil)

	mockClient.AssertExpectations(c)
}

func (s *ClientSuite) TestGetPersistedErrorCases(c *gc.C) {
	mockClient := &mockHttpClient{}
	s.client.httpClient = mockClient

	location := newURL("http://cloud/location")
	readResult := journal.ReadResult{Offset: 1005, WriteHead: 3000, Fragment: fragmentFixture}

	// Expect response errors are passed through.
	mockClient.On("Get", "http://cloud/location").Return(nil, errors.New("error!")).Once()

	body, err := s.client.openFragment(location, readResult)
	c.Check(body, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "error!")

	// Expect non-200 is turned into an error.
	mockClient.On("Get", "http://cloud/location").Return(&http.Response{
		StatusCode: http.StatusTeapot,
		Status:     "error!",
		Body:       ioutil.NopCloser(nil),
	}, nil).Once()

	body, err = s.client.openFragment(location, readResult)
	c.Check(body, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "fetching fragment: error!")

	// Seek failure (too little content). Expect error is returned.
	mockClient.On("Get", "http://cloud/location").Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("abc")),
	}, nil).Once()

	body, err = s.client.openFragment(location, readResult)
	c.Check(body, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "seeking fragment: EOF")
}

func (s *ClientSuite) TestCreate(c *gc.C) {
	mockClient := &mockHttpClient{}

	// Expect a POST of the journal. Fail with a conflict.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "POST" &&
			request.URL.String() == "http://default/a/journal"
	})).Return(&http.Response{
		StatusCode: http.StatusConflict,
		Body:       ioutil.NopCloser(nil),
	}, nil).Once()

	// Expect a second POST, which succeeds.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "POST" &&
			request.URL.String() == "http://default/a/journal"
	})).Return(&http.Response{
		StatusCode: http.StatusCreated,
		Body:       ioutil.NopCloser(nil),
	}, nil).Once()

	s.client.httpClient = mockClient
	c.Check(s.client.Create("a/journal"), gc.Equals, journal.ErrExists)
	c.Check(s.client.Create("a/journal"), gc.IsNil)

	mockClient.AssertExpectations(c)
}

func (s *ClientSuite) TestPut(c *gc.C) {
	content := strings.NewReader("foobar")
	mockClient := &mockHttpClient{}

	// Expect a HEAD request at offset=-1 (not satisfiable) to fill the location cache.
	// Return an error, and expect it's passed through.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.Host == "default" &&
			request.URL.Path == "/a/journal" &&
			request.URL.Query().Get("offset") == "-1"
	})).Return(&http.Response{
		Status:     "Teapot",
		StatusCode: http.StatusTeapot,
		Body:       ioutil.NopCloser(bytes.NewBufferString("head failed reason")),
	}, nil).Once()

	s.client.httpClient = mockClient
	res := s.client.Put(journal.AppendArgs{Journal: "a/journal", Content: content})
	c.Check(res.Error, gc.ErrorMatches, `Teapot \(head failed reason\)`)

	// Again. This time, return ErrNotYetAvailable (expected because it's non-blocking).
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.Host == "default" &&
			request.URL.Path == "/a/journal" &&
			request.URL.Query().Get("offset") == "-1"
	})).Return(&http.Response{
		StatusCode: http.StatusRequestedRangeNotSatisfiable,
		Request:    &http.Request{URL: newURL("http://redirected-server/a/journal")},
		Body:       ioutil.NopCloser(nil),
	}, nil).Once()

	// Expect a PUT to the redirected server, which returns an error.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "PUT" &&
			request.URL.Host == "redirected-server" &&
			request.URL.Path == "/a/journal" &&
			request.ContentLength == 6
	})).Return(&http.Response{
		Status:     "Internal Server Error",
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(bytes.NewBufferString("some reason")),
		Header: http.Header{
			WriteHeadHeader: []string{"12341234"},
		},
	}, nil).Once()

	res = s.client.Put(journal.AppendArgs{Journal: "a/journal", Content: content})
	c.Check(res.Error, gc.ErrorMatches, `Internal Server Error \(some reason\)`)

	// WriteHead was parsed despite the failure.
	c.Check(res.WriteHead, gc.Equals, int64(12341234))

	// The broker will say that the write has failed, so we shouldn't hit the
	// counters for this journal. It has yet to be used at all, so it shouldn't
	// exist yet.
	c.Check(gazetteMap.Get("writers").(*expvar.Map).Get("a/journal"), gc.IsNil)

	// Expect a PUT, and this time return success.
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "PUT" &&
			// Cache is cleared, so "default" pops up again.
			request.URL.Host == "redirected-server" &&
			request.URL.Path == "/a/journal" &&
			request.ContentLength == 6
	})).Return(&http.Response{
		StatusCode: http.StatusNoContent, // Indicates success.
		Body:       ioutil.NopCloser(nil),
		Header: http.Header{
			WriteHeadHeader: []string{"12341235"},
		},
	}, nil).Run(func(args mock.Arguments) {
		request := args[0].(*http.Request)
		c.Check(request.Body, gc.DeepEquals, ioutil.NopCloser(content))
	}).Once()

	res = s.client.Put(journal.AppendArgs{Journal: "a/journal", Content: content})
	c.Check(res.Error, gc.IsNil)
	c.Check(res.WriteHead, gc.Equals, int64(12341235))
	mockClient.AssertExpectations(c)

	// Write success. Expect that the write stats were published to the
	// counters. ("content" is 6 bytes long, and is reflected in the bytes
	// counter.)
	writerMap := gazetteMap.Get("writers").(*expvar.Map).Get("a/journal").(*expvar.Map)
	c.Check(writerMap.Get("bytes").(*expvar.Int).String(), gc.Equals, "6")
	c.Check(writerMap.Get("head").(*expvar.Int).String(), gc.Equals, "12341235")
}

func (s *ClientSuite) TestReadResultParsingErrorCases(c *gc.C) {
	args := journal.ReadArgs{Journal: "a/journal"}

	{ // Expect 416 is mapped into ErrNotYetAvailable.
		response := newReadResponseFixture()
		response.StatusCode = http.StatusRequestedRangeNotSatisfiable

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.Equals, journal.ErrNotYetAvailable)
	}
	{ // Missing Content-Range.
		response := newReadResponseFixture()
		response.Header.Del("Content-Range")

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, "expected Content-Range header")
	}
	{ // Malformed Content-Range.
		response := newReadResponseFixture()
		response.Header.Set("Content-Range", "foobar")

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, "invalid Content-Range: foobar")
	}
	{ // Missing Write-Head.
		response := newReadResponseFixture()
		response.Header.Del(WriteHeadHeader)

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, "expected "+WriteHeadHeader+" header")
	}
	{ // Malformed Write-Head.
		response := newReadResponseFixture()
		response.Header.Set(WriteHeadHeader, "foobar")

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, "parsing "+WriteHeadHeader+": .*")
	}
	{ // Malformed fragment name.
		response := newReadResponseFixture()
		response.Header.Set(FragmentNameHeader, "foobar")

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, "parsing "+FragmentNameHeader+": .*")
	}
	{ // Malformed fragment location.
		response := newReadResponseFixture()
		response.Header.Set(FragmentLocationHeader, "@$%!@#@3")

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, "parsing "+FragmentLocationHeader+": .*")
	}
	{ // Non-206 response.
		response := newReadResponseFixture()
		response.StatusCode = http.StatusInternalServerError
		response.Status = "500 Internal Error"
		response.Body = ioutil.NopCloser(strings.NewReader("server error!"))

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.ErrorMatches, `500 Internal Error \(server error!\)`)
	}
}

func (s *ClientSuite) TestAppendResultParsingErrorCases(c *gc.C) {
	response := newReadResponseFixture()
	response.StatusCode = http.StatusGone

	c.Check(s.client.parseAppendResponse(response).Error, gc.Equals, journal.ErrNotBroker)
}

func (s *ClientSuite) TestBuildReadURL(c *gc.C) {
	args := journal.ReadArgs{
		Journal:  "a/journal",
		Blocking: true,
		Deadline: time.Now().Add(10 * time.Millisecond),
	}
	url := s.client.buildReadURL(args)
	c.Check(strings.Contains(url.String(), "block=true"), gc.Equals, true)
	c.Check(strings.Contains(url.String(), "blockms="), gc.Equals, true)
	c.Check(strings.Contains(url.String(), "blockms=0"), gc.Equals, false)

	args = journal.ReadArgs{Journal: "a/journal"}
	url = s.client.buildReadURL(args)
	c.Check(strings.Contains(url.String(), "block=false"), gc.Equals, true)
	c.Check(strings.Contains(url.String(), "blockms="), gc.Equals, false)
}

// Regression test for issue #890.
func (s *ClientSuite) TestDialerIsNonNil(c *gc.C) {
	// What we really want to test is that TCP keep-alive is set. There isn't a
	// great way to test this, as Dial is a closure over net.Dialer. At least
	// satisfy ourselves that a non-default dialer is used (a requirement for
	// automatic setting of TCP keep-alive).
	client, _ := NewClient("http://default")
	c.Check(client.httpClient.(*http.Client).Transport.(*http.Transport).Dial, gc.NotNil)
}

func (s *ClientSuite) TestFragmentBeforeTime(c *gc.C) {
	var mockClient = new(mockHttpClient)
	var response = newReadResponseFixture()
	var baseDate = time.Date(2016, 7, 12, 23, 0, 0, 0, time.UTC)

	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		c.Log(request.Method + " " + request.URL.String())

		// Every fragment contains 1000 bytes, and is timed an hour after the
		// last fragment.
		var off, err = strconv.Atoi(request.URL.Query()["offset"][0])
		c.Assert(err, gc.IsNil)
		var fragmentIndex = int64(off) / 1000

		var fragment = journal.Fragment{
			Begin:         fragmentIndex * 1000,
			End:           (fragmentIndex + 1) * 1000,
			RemoteModTime: baseDate.Add(time.Hour * time.Duration(fragmentIndex)),
			Sum:           fakeSum,
		}

		response.Header.Set(FragmentNameHeader, fragment.ContentName())
		response.Header.Set(FragmentLastModifiedHeader, fragment.RemoteModTime.Format(http.TimeFormat))

		return true
	})).Return(response, nil).Times(14)

	// Original request's fragment is picked, because the next one's timestamp
	// exceeds the requested time.
	s.client.httpClient = mockClient
	frag, err := s.client.FragmentBeforeTime("a/journal",
		time.Date(2016, 7, 13, 0, 46, 0, 0, time.UTC))

	var expect = fragmentFixture
	expect.RemoteModTime = baseDate.Add(time.Hour)
	c.Assert(err, gc.IsNil)
	c.Check(frag, gc.DeepEquals, expect)

	mockClient.AssertExpectations(c)
}

func (s *ClientSuite) TestFragmentBeforeTimeNoMatch(c *gc.C) {
	// Simulate response when there is no fragment. Should behave well.
	var response = newReadResponseFixture()
	response.Header.Del(FragmentNameHeader)
	response.Header.Del(FragmentLastModifiedHeader)

	var mockClient = new(mockHttpClient)
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		c.Log(request.Method + " " + request.URL.String())
		return request.Method == "HEAD"
	})).Return(response, nil)

	s.client.httpClient = mockClient
	frag, err := s.client.FragmentBeforeTime("a/journal",
		time.Date(2016, 7, 12, 23, 45, 59, 0, time.UTC))

	c.Assert(err, gc.IsNil)
	c.Check(frag, gc.DeepEquals, journal.Fragment{})
}

func (s *ClientSuite) TestFragmentsInRange(c *gc.C) {
	var mockClient = new(mockHttpClient)
	var response = newReadResponseFixture()

	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		c.Log(request.Method + " " + request.URL.String())
		var offStr = request.URL.Query()["offset"][0]

		// Round |off| to nearest multiple of 1000 (e.g., the broker has 1000
		// byte fragments starting from 0, no matter what user asks for.)
		off, _ := strconv.Atoi(offStr)
		off -= off % 1000
		var fakeFrag = journal.Fragment{
			Begin: int64(off),
			End:   int64(off + 1000),
			Sum:   [...]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}
		response.Header.Set(FragmentNameHeader, fakeFrag.ContentName())
		response.Header.Del(FragmentLastModifiedHeader)

		return request.Method == "HEAD" && request.URL.Path == "/a/journal"
	})).Return(response, nil).Times(5)

	s.client.httpClient = mockClient
	frags, err := s.client.FragmentsInRange("a/journal", 1001, 5999)
	c.Assert(err, gc.IsNil)
	c.Check(frags, gc.DeepEquals, []journal.Fragment{
		{Journal: "a/journal", Begin: 1000, End: 2000},
		{Journal: "a/journal", Begin: 2000, End: 3000},
		{Journal: "a/journal", Begin: 3000, End: 4000},
		{Journal: "a/journal", Begin: 4000, End: 5000},
		{Journal: "a/journal", Begin: 5000, End: 6000},
	})

	mockClient.AssertExpectations(c)
}

func newURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

var _ = gc.Suite(&ClientSuite{})
