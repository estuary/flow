package gazette

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
)

// A parsing DNS caching layer on a string URL.
type CachedURL struct {
	// Accessible, absolute URL of this endpoint.
	Base string

	// Parsed URL with cached IP-resolution applied.
	cachedURL   *url.URL
	cachedURLMu sync.Mutex
}

func (ep *CachedURL) NewHTTPRequest(method, uri string, body io.Reader) (
	*http.Request, error) {

	if resolvedUrl, err := ep.ResolveURL(); err != nil {
		return nil, err
	} else {
		return http.NewRequest(method, resolvedUrl.String()+uri, body)
	}
}

func (ep *CachedURL) URL() (*url.URL, error) {
	if url, err := url.Parse(ep.Base); err != nil {
		return nil, err
	} else if !url.IsAbs() {
		return nil, errors.New("not an absolute URL: " + ep.Base)
	} else {
		return url, nil
	}
}

func (ep *CachedURL) ResolveURL() (*url.URL, error) {
	ep.cachedURLMu.Lock()
	defer ep.cachedURLMu.Unlock()

	if ep.cachedURL != nil {
		return ep.cachedURL, nil
	}

	if url, err := ep.URL(); err != nil {
		return nil, err
	} else if r, err := net.ResolveTCPAddr("tcp", url.Host); err != nil {
		return nil, err
	} else {
		url.Host = r.String()
		ep.cachedURL = url
		return url, nil
	}
}

func (ep *CachedURL) InvalidateResolution() {
	ep.cachedURLMu.Lock()
	defer ep.cachedURLMu.Unlock()

	ep.cachedURL = nil
}
