// Package consumermodule defines the API by which consumer applications, compiled
// as dynamic libraries, may be loaded, initialized, and executed by the
// `run-consumer` binary. Specifically:
//
//  - An application must be built as a go plugin
//    (eg `go build --buildmode=plugin`; see https://golang.org/pkg/plugin/)
//  - The application must define an exported var `Module` of type `Module`. The
//    API contract defines mechanisms for configuration, initialization,
//    and tear-down of the application.
package consumermodule

import (
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
)

// Module is the interface implemented by consumer application modules.
type Module interface {
	// NewConfig returns a new Config.
	NewConfig() Config
	// NewApplication returns a new instance of the consumer.Application.
	NewApplication(Config) consumer.Application
	// Register any additional services implemented by the consumer module
	// onto the provided ServerContext. The consumer.Service may be used
	// to support Shard resolution and request proxying.
	Register(Config, consumer.Application, mbp.ServerContext, *consumer.Service)
}

// Config is the top-level configuration object of a Gazette consumer. It must
// be parse-able by `go-flags`, and must present a BaseConfig.
type Config interface {
	// GetBaseConfig of the Config.
	GetBaseConfig() BaseConfig
}

// BaseConfig is the top-level configuration object of a Gazette consumer.
type BaseConfig struct {
	Consumer struct {
		Module string `long:"module" env:"MODULE" description:"Path to consumer module to dynamically load"`

		mbp.ServiceConfig

		Limit uint32 `long:"limit" env:"LIMIT" default:"32" description:"Maximum number of Shards this consumer process will allocate"`
	} `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`

	Broker mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`

	Etcd struct {
		mbp.EtcdConfig

		Prefix string `long:"prefix" env:"PREFIX" description:"Etcd prefix for consumer state and coordination (eg, /gazette/consumers/myApplication)"`
	} `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`

	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

// GetBaseConfig returns itself, and trivially implements the Config interface.
func (c BaseConfig) GetBaseConfig() BaseConfig { return c }
