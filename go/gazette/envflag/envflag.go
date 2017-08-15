// Package envflag implements support for setting flags through environment
// variables.
//
// TODO(rupert): Write tests and examples.
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

// items holds the list of all defined envflags
var items []parseableFromEnv

// serviceEndpointItem holds the metadata for an envflag representing a service
// endpoint. A service endpoint is a host-port pair.
type serviceEndpointItem struct {
	// host and port are the pair of names of the environment variables.
	host, port string
	// ptr is the address of the string variable that stores the value of the
	// flag. This typically is the return value of a flag.String call.
	ptr *string
}

// parseFromEnv implements parseableFromEnv
func (i serviceEndpointItem) parseFromEnv() {
	if h := os.Getenv(i.host); h != "" {
		if p := os.Getenv(i.port); p != "" {
			*i.ptr = h + ":" + p
		}
	}
}

// ServiceEndpoint defines a service endpoint flag. A service endpoint is a
// host-port value, parsed either from a pair of environment variables
// "<NAME>_SERVICE_HOST", "<NAME>_SERVICE_PORT" or a single colon-delimited
// command-line argument "-<name>". The return value is the address of a string
// variable that stores the value of the flag.
func ServiceEndpoint(name, value, usage string) *string {
	var prefix = strings.ToUpper(name)
	var evHost = prefix + "_SERVICE_HOST"
	var evPort = prefix + "_SERVICE_PORT"

	var ptr = flag.String(flagPrefix+name+"Endpoint", value, fmt.Sprintf("%s (%s, %s)", usage, evHost, evPort))
	items = append(items, serviceEndpointItem{evHost, evPort, ptr})
	return ptr
}

// stringItem holds the metadata for an envflag representing a simple string. A
// simple string envflag attempts to parse its value from a single environment
// variable.
type stringItem struct {
	// envvarName is the name of the environment variable.
	envvarName string
	// ptr is the address of the string variable that stores the value of the
	// flag. This typically is the return value of a flag.String call.
	ptr *string
}

// parseFromEnv implements parseableFromEnv
func (i stringItem) parseFromEnv() {
	var val = os.Getenv(i.envvarName)
	if val != "" {
		*i.ptr = val
	}
}

// String defines a string flag. A string flag is a single string value, parsed
// either from an environment variable or a command-line argument). The return
// value is the address of a string variable that stores the value of the flag.
func String(flagName, envvarName, value, usage string) *string {
	var ptr = flag.String(flagPrefix+flagName, value, fmt.Sprintf("%s (%s)", usage, envvarName))

	items = append(items, stringItem{envvarName, ptr})
	return ptr
}

// Parse parses configuration from environment variables and stores non-empty
// values. It is recommended to call this after all flags are defined and
// before flag.Parse.
func Parse() {
	for _, entry := range items {
		entry.parseFromEnv()
	}
}
