package gazette

import (
	"net/http"

	"golang.org/x/net/trace"
)

// maybeTrace instruments specific Requests for tracing. At present, all
// requests are traced. In the future, we may want to flag this, or
// selectively trace certain journals, or enable with URL query arguments,
// or some combination of these.
func maybeTrace(r *http.Request, api string) *http.Request {
	var tr = trace.New(api, r.URL.Path)
	tr.LazyPrintf("RemoteAddr: %s", r.RemoteAddr)
	return r.WithContext(trace.NewContext(r.Context(), tr))
}

// finishTrace finishes a running trace of the request, if it exists.
// It is intended to be defered immediately after a `maybeTrace`.
func finishTrace(r *http.Request) {
	if tr, ok := trace.FromContext(r.Context()); ok {
		tr.Finish()
	}
}
