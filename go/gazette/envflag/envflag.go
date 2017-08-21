// Package envflag implements support for setting flags through environment
// variables. Unlike the package standard flag package, a set of top-level
// wrapper functions are not provided.
package envflag

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// flagPrefix is prefixed to the flag name to avoid conflicts with existing
// flags.
//
// TODO(rupert): Remove this when package endpoints is fully decoupled.
const flagPrefix = "oss"

type parseableFromEnv interface {
	parseFromEnv()
}

// serviceEndpointFlag holds the metadata for an envflag representing a service
// endpoint. A service endpoint is a host-port pair.
type serviceEndpointFlag struct {
	// host and port are the pair of names of the environment variables.
	host, port string
	// ptr is the address of the string variable that stores the value of the
	// flag. This typically is the return value of a flag.String call.
	ptr *string
}

// parseFromEnv implements parseableFromEnv
func (f serviceEndpointFlag) parseFromEnv() {
	if h := os.Getenv(f.host); h != "" {
		if p := os.Getenv(f.port); p != "" {
			*f.ptr = h + ":" + p
		}
	}
}

// stringFlag holds the metadata for an envflag representing a simple string. A
// simple string envflag attempts to parse its value from a single environment
// variable.
type stringFlag struct {
	// envvarName is the name of the environment variable.
	envvarName string
	// ptr is the address of the string variable that stores the value of the
	// flag. This typically is the return value of a flag.String call.
	ptr *string
}

// parseFromEnv implements parseableFromEnv
func (f stringFlag) parseFromEnv() {
	var val = os.Getenv(f.envvarName)
	if val != "" {
		*f.ptr = val
	}
}

// FlagSet represents a set of defined env flags.
type FlagSet struct {
	fs     *flag.FlagSet
	formal map[string]parseableFromEnv
}

// NewFlagSet returns a new, empty flag set.
func NewFlagSet(fs *flag.FlagSet) *FlagSet {
	return &FlagSet{fs: fs}
}

// ServiceEndpoint defines a service endpoint flag. A service endpoint is a
// host-port value, parsed either from a pair of environment variables
// "<NAME>_SERVICE_HOST", "<NAME>_SERVICE_PORT" or a single colon-delimited
// command-line argument "-oss<name>Endpoint". The return value is the address
// of a string variable that stores the value of the flag.
func (fs *FlagSet) ServiceEndpoint(name, value, usage string) *string {
	var prefix = strings.ToUpper(name)
	var evHost = prefix + "_SERVICE_HOST"
	var evPort = prefix + "_SERVICE_PORT"

	var ptr = fs.fs.String(flagPrefix+name+"Endpoint", value, fmt.Sprintf("%s (%s, %s)", usage, evHost, evPort))
	fs.addFlag(name, serviceEndpointFlag{evHost, evPort, ptr})
	return ptr
}

// String defines a string flag. A string flag is a single string value, parsed
// either from an environment variable "<envvarName>" or a command-line
// argument "-oss<flagName". The return value is the address of a string
// variable that stores the value of the flag.
//
// This is a general flag creation utility which is why flagName and
// envvarName are provided separately rather than generated from a common base.
func (fs *FlagSet) String(flagName, envvarName, value, usage string) *string {
	var ptr = fs.fs.String(flagPrefix+flagName, value, fmt.Sprintf("%s (%s)", usage, envvarName))

	fs.addFlag(envvarName, stringFlag{envvarName, ptr})
	return ptr
}

func (fs *FlagSet) addFlag(name string, flag parseableFromEnv) {
	if fs.formal == nil {
		fs.formal = make(map[string]parseableFromEnv)
	}
	fs.formal[name] = flag
}

// Parse parses configuration from environment variables and stores non-empty
// values. It is recommended to call this after all flags are defined and
// before flag.FlagSet.Parse.
func (fs *FlagSet) Parse() {
	for _, f := range fs.formal {
		f.parseFromEnv()
	}
}

// CommandLine is the default set of env flags, backed by the default set of
// command-line flags. The top-level functions of this package are wrappers for
// the methods of CommandLine.
var CommandLine = NewFlagSet(flag.CommandLine)
