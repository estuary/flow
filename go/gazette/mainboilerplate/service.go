package mainboilerplate

import (
	"fmt"

	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/protocol"
)

// ZoneConfig configures the zone of the application.
type ZoneConfig struct {
	Zone string `long:"zone" env:"ZONE" default:"local" description:"Availability zone within which this process is running"`
}

// ServiceConfig represents identification and addressing configuration of the process.
type ServiceConfig struct {
	ZoneConfig
	ID   string `long:"id" env:"ID" default:"localhost" description:"Unique ID of the process"`
	Host string `long:"host" env:"HOST" default:"localhost" description:"Addressable, advertised hostname of this process"`
	Port uint16 `long:"port" env:"PORT" default:"8080" description:"Service port for HTTP and gRPC requests"`
}

// ProcessSpec of the ServiceConfig.
func (cfg ServiceConfig) ProcessSpec() protocol.ProcessSpec {
	return protocol.ProcessSpec{
		Id:       protocol.ProcessSpec_ID{Zone: cfg.Zone, Suffix: cfg.ID},
		Endpoint: protocol.Endpoint(fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)),
	}
}

// MemberKey of an allocator implied by the ServiceConfig.
func (cfg ServiceConfig) MemberKey(ks *keyspace.KeySpace) string {
	return allocator.MemberKey(ks, cfg.Zone, cfg.ID)
}
