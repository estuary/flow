package journal

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"

	gc "github.com/go-check/check"
)

type ProtocolSuite struct{}

func (s *ProtocolSuite) TestErrorsAsStatusCodes(c *gc.C) {
	// Round-trip each protocol error.
	for _, err := range protocolErrors {
		var response = http.Response{
			StatusCode: StatusCodeForError(err),
			Status:     "status!",
			Body:       ioutil.NopCloser(nil),
		}
		c.Check(ErrorFromResponse(&response), gc.Equals, err)
	}

	// Known success codes.
	for _, code := range []int{
		http.StatusPartialContent,
		http.StatusNoContent,
		http.StatusCreated} {

		var response = http.Response{StatusCode: code, Body: ioutil.NopCloser(nil)}
		c.Check(ErrorFromResponse(&response), gc.Equals, nil)
	}

	// A novel error.
	c.Check(StatusCodeForError(errors.New("error!")), gc.Equals,
		http.StatusInternalServerError)

	// A novel, unknown status code.
	var response = http.Response{
		StatusCode: http.StatusTeapot,
		Status:     "error!",
		Body:       ioutil.NopCloser(bytes.NewBufferString("body")),
	}
	c.Check(ErrorFromResponse(&response), gc.ErrorMatches, `error! \(body\)`)
}

var _ = gc.Suite(&ProtocolSuite{})
