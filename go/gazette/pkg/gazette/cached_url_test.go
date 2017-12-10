package gazette

import (
	gc "github.com/go-check/check"
)

type CachedURLSuite struct{}

func (s *CachedURLSuite) TestURLChecks(c *gc.C) {
	ep := CachedURL{Base: "://invalid"}
	_, err := ep.URL()
	c.Check(err, gc.ErrorMatches, "parse .*")

	ep = CachedURL{Base: "/foo/bar"}
	_, err = ep.URL()
	c.Check(err, gc.ErrorMatches, "not an absolute URL:.*")

	ep = CachedURL{Base: "https://foo/bar"}
	url, err := ep.URL()
	c.Check(url, gc.NotNil)
	c.Check(err, gc.IsNil)
}

func (s *CachedURLSuite) TestResolution(c *gc.C) {
	ep := CachedURL{Base: "http://localhost:9000/a/route"}

	resolvedURL, err := ep.ResolveURL()
	c.Check(err, gc.IsNil)
	c.Check(resolvedURL.String(), gc.Equals, "http://127.0.0.1:9000/a/route")

	// Subsequent resolutions use a cached copy.
	reResolvedUrl, err := ep.ResolveURL()
	c.Check(resolvedURL, gc.Equals, reResolvedUrl)

	// Until the resolution is invalidated, at which point it's re-resolved.
	ep.InvalidateResolution()
	reResolvedUrl, err = ep.ResolveURL()
	c.Check(resolvedURL, gc.Not(gc.Equals), reResolvedUrl)
}

var _ = gc.Suite(&CachedURLSuite{})
