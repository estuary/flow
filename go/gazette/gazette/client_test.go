package gazette

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	. "github.com/pippio/gazette/journal"
)

const kFragmentFixtureStr = "00000000000003e8-00000000000007d0-" +
	"0102030405060708090a0b0c0d0e0f1011121314"

var fragmentFixture = Fragment{
	Journal: "a/journal",
	Begin:   1000,
	End:     2000,
	Sum:     [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
}

type ClientSuite struct {
	client *Client
}

func (s *ClientSuite) SetUpTest(c *gc.C) {
	client, err := NewClient("http://default")
	c.Assert(err, gc.IsNil)
	s.client = client
}

func newReadResponseFixture() *http.Response {
	return &http.Response{
		// Return a successful HEAD response, which was redirected from http://default.
		StatusCode: http.StatusPartialContent,
		Header: http.Header{
			"Content-Range":        []string{"bytes 1005-9999999999/9999999999"},
			WriteHeadHeader:        []string{"3000"},
			FragmentNameHeader:     []string{kFragmentFixtureStr},
			FragmentLocationHeader: []string{"http://cloud/fragment/location"},
		},
		Request: &http.Request{
			URL: newURL("http://redirected-server/a/journal"),
		},
		Body: ioutil.NopCloser(strings.NewReader("body")),
	}
}

func (s *ClientSuite) TestHeadRequest(c *gc.C) {
	mockClient := &mockHttpClient{}

	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(newReadResponseFixture(), nil).Once()

	s.client.httpClient = mockClient
	result, loc := s.client.Head(ReadArgs{Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result, gc.DeepEquals, ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	c.Check(loc, gc.DeepEquals, newURL("http://cloud/fragment/location"))

	mockClient.AssertExpectations(c)

	// Expect that the redirected location was cached.
	cached, _ := s.client.locationCache.Get("/a/journal")
	c.Check(cached, gc.DeepEquals, newURL("http://redirected-server/a/journal"))
}

func (s *ClientSuite) TestDirectGet(c *gc.C) {
	mockClient := &mockHttpClient{}

	responseFixture := newReadResponseFixture()
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "GET" &&
			request.URL.String() == "http://default/a/journal?block=false&offset=1005"
	})).Return(responseFixture, nil).Once()

	s.client.httpClient = mockClient
	result, body := s.client.GetDirect(ReadArgs{
		Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result, gc.DeepEquals, ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	mockClient.AssertExpectations(c)

	// Expect server's response body is directly passed through.
	c.Check(body, gc.Equals, responseFixture.Body)
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
			request.URL.String() == "http://redirected-server/a/journal?block=true&offset=1005"
	})).Return(responseFixture, nil).Once()

	s.client.httpClient = mockClient
	result, body := s.client.Get(ReadArgs{Journal: "a/journal", Offset: 1005, Blocking: true})

	c.Check(result, gc.DeepEquals, ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	mockClient.AssertExpectations(c)

	// Expect server's response body is directly passed through.
	c.Check(body, gc.Equals, responseFixture.Body)
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
	result, body := s.client.Get(ReadArgs{Journal: "a/journal", Offset: 1005, Blocking: false})

	c.Check(result.Error, gc.IsNil)
	c.Check(result, gc.DeepEquals, ReadResult{
		Offset:    1005,
		WriteHead: 3000,
		Fragment:  fragmentFixture,
	})
	mockClient.AssertExpectations(c)

	// Expect that the returned response is pre-seeked to the correct offset.
	data, _ := ioutil.ReadAll(body)
	c.Check(string(data), gc.Equals, "fragment-content...")
}

func (s *ClientSuite) TestGetPersistedErrorCases(c *gc.C) {
	mockClient := &mockHttpClient{}
	s.client.httpClient = mockClient

	location := newURL("http://cloud/location")
	readResult := ReadResult{Offset: 1005, WriteHead: 3000, Fragment: fragmentFixture}

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

func (s *ClientSuite) TestPut(c *gc.C) {
	content := strings.NewReader("foobar")

	mockClient := &mockHttpClient{}
	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "HEAD" &&
			request.URL.Host == "default" &&
			request.URL.Path == "/a/journal"
	})).Return(newReadResponseFixture(), nil)

	mockClient.On("Do", mock.MatchedBy(func(request *http.Request) bool {
		return request.Method == "PUT" &&
			request.URL.Host == "redirected-server" &&
			request.URL.Path == "/a/journal"
	})).Return(&http.Response{
		StatusCode: http.StatusNoContent, // Indicates success.
		Request:    &http.Request{URL: newURL("http://default/a/journal")},
		Body:       ioutil.NopCloser(nil),
	}, nil).Run(func(args mock.Arguments) {

		request := args[0].(*http.Request)
		c.Check(request.Body, gc.DeepEquals, ioutil.NopCloser(content))
	}).Once()

	s.client.httpClient = mockClient
	c.Check(s.client.Put(AppendArgs{Journal: "a/journal", Content: content}), gc.IsNil)
	mockClient.AssertExpectations(c)
}

func (s *ClientSuite) TestReadResultParsingErrorCases(c *gc.C) {
	args := ReadArgs{Journal: "a/journal"}

	{ // Expect 416 is mapped into ErrNotYetAvailable.
		response := newReadResponseFixture()
		response.StatusCode = http.StatusRequestedRangeNotSatisfiable

		result, _ := s.client.parseReadResult(args, response)
		c.Check(result.Error, gc.Equals, ErrNotYetAvailable)
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
	{ // Non-204 response.
		response := newReadResponseFixture()
		response.StatusCode = http.StatusNotFound
		response.Status = "404 Not Found"
		response.Body = ioutil.NopCloser(strings.NewReader("not found"))

		c.Check(s.client.parseAppendResponse(response), gc.ErrorMatches,
			`404 Not Found \(not found\)`)
	}
}

func newURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

var _ = gc.Suite(&ClientSuite{})
