package envflag_test

import (
	"flag"
	"fmt"

	"github.com/pippio/gazette/envflag"
)

func Example() {
	// Define envflags. Because this example is testable, long descriptive
	// names have been chosen to reduce the chances of an environment variable
	// by that name actually existing.
	dbEndpoint := envflag.CommandLine.ServiceEndpoint(
		"envflagExampleMysql", "mysql.example:3306", "MySQL endpoint")
	redisEndpoint := envflag.CommandLine.ServiceEndpoint(
		"envflagExampleRedis", "redis.example:6379", "Redis endpoint")
	pingURL := envflag.CommandLine.String(
		"ping", "ENVFLAG_EXAMPLE_PING_URL", "http://localhost/ping", "URL to ping")
	user := envflag.CommandLine.String(
		"user", "ENVFLAG_EXAMPLE_USER", "nobody", "User to act as")

	// Parse values from the environment. This is typically done before parsing
	// from the command-line arguments so the command-line takes precedence
	// over the environment.
	envflag.CommandLine.Parse()

	// It is the responsibility of the user of envflag to call flag.Parse to
	// parse command-line flags.
	flag.Parse()

	fmt.Println(*dbEndpoint)
	fmt.Println(*redisEndpoint)
	fmt.Println(*pingURL)
	fmt.Println(*user)

	// Output:
	// mysql.example:3306
	// redis.example:6379
	// http://localhost/ping
	// nobody
}

func ExampleFlagSet_ServiceEndpoint() {
	// For now, command-line flag names are prefixed with "oss" to avoid
	// collisions. This will be removed before final release.
	fs := flag.NewFlagSet("Example", flag.PanicOnError)
	efs := envflag.NewFlagSet(fs)

	efs.ServiceEndpoint("api", "localhost:80", "API endpoint")
	fmt.Println(fs.Lookup("ossapiEndpoint").Name)

	// Output:
	// ossapiEndpoint
}

func ExampleFlagSet_String() {
	// For now, command-line flag names are prefixed with "oss" to avoid
	// collisions. This will be removed before final release.
	fs := flag.NewFlagSet("Example", flag.PanicOnError)
	efs := envflag.NewFlagSet(fs)

	efs.String("callback", "CALLBACK_URL", "http://my.example/callback", "HTTP callback")
	fmt.Println(fs.Lookup("callback").Name)

	// Output:
	// callback
}
