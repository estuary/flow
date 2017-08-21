package envflag

import (
	"flag"
	"os"
	"testing"

	"github.com/go-check/check"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type envflagSuite struct {
}

var _ = check.Suite(&envflagSuite{})

func (s *envflagSuite) TestServiceEndpoint(c *check.C) {
	var fs = flag.NewFlagSet("TestServiceEndpoint", flag.ContinueOnError)
	var efs = NewFlagSet(fs)
	var sut = efs.ServiceEndpoint("dummyName", "default value", "Foo bar baz")

	// Envflags create underlying flags.
	var actualFlag = fs.Lookup("ossdummyNameEndpoint")
	c.Assert(actualFlag, check.NotNil)
	c.Check(actualFlag.DefValue, check.Equals, "default value")
	c.Check(actualFlag.Usage, check.Equals, "Foo bar baz (DUMMYNAME_SERVICE_HOST, DUMMYNAME_SERVICE_PORT)")

	// Service endpoint flags parse from a pair of environment variables
	defer assertAndSetenv(c, "DUMMYNAME_SERVICE_HOST", "dummy.example")()
	defer assertAndSetenv(c, "DUMMYNAME_SERVICE_PORT", "1234")()

	// Verify pre- and post-Parse values.
	c.Check(*sut, check.Equals, "default value")
	efs.Parse()
	c.Check(*sut, check.Equals, "dummy.example:1234")
}

func (s *envflagSuite) TestString(c *check.C) {
	var fs = flag.NewFlagSet("TestString", flag.ContinueOnError)
	var efs = NewFlagSet(fs)
	var sut = efs.String("dummyName", "DUMMYNAME_URL", "http://default.example/hello", "Foo bar baz")

	// String creates underlying flag.
	var actualFlag = fs.Lookup("ossdummyName")
	c.Assert(actualFlag, check.NotNil)
	c.Check(actualFlag.DefValue, check.Equals, "http://default.example/hello")
	c.Check(actualFlag.Usage, check.Equals, "Foo bar baz (DUMMYNAME_URL)")

	// String flags parse from the environment
	defer assertAndSetenv(c, "DUMMYNAME_URL", "https://api.example/hello")()

	c.Check(*sut, check.Equals, "http://default.example/hello")
	efs.Parse()
	c.Check(*sut, check.Equals, "https://api.example/hello")
}

// unsetterFunc is a callback to unset an environment variable.
type unsetterFunc func()

// assertAndSetenv is a utility for setting and unsetting environment variables
// after asserting they did not already exist.
//
// A typical usage would perform everything in one line. Note the trailing
// parens.
//
//     defer assertAndSetenv(c, "my-key", "my-value")()
func assertAndSetenv(c *check.C, key, value string) unsetterFunc {
	var _, hasKey = os.LookupEnv(key)
	c.Assert(hasKey, check.Equals, false)

	os.Setenv(key, value)
	return func() {
		os.Unsetenv(key)
	}
}
