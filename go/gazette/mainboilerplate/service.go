package mainboilerplate

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	petname "github.com/dustinkirkland/golang-petname"
	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
)

// ZoneConfig configures the zone of the application.
type ZoneConfig struct {
	Zone string `long:"zone" env:"ZONE" default:"local" description:"Availability zone within which this process is running"`
}

// ServiceConfig represents identification and addressing configuration of the process.
type ServiceConfig struct {
	ZoneConfig
	ID   string `long:"id" env:"ID" description:"Unique ID of this process. Auto-generated if not set"`
	Host string `long:"host" env:"HOST" description:"Addressable, advertised hostname or IP of this process. Hostname is used if not set"`
	Port string `long:"port" env:"PORT" description:"Service port for HTTP and gRPC requests. A random port is used if not set. Port may also take the form 'unix:///path/to/socket' to use a Unix Domain Socket"`
}

// ProcessSpec of the ServiceConfig.
func (cfg ServiceConfig) BuildProcessSpec(srv *server.Server) protocol.ProcessSpec {
	var err error
	if cfg.ID == "" {
		rand.Seed(time.Now().UnixNano()) // Seed generator for Generate's use.
		cfg.ID = petname.Generate(2, "-")
	}
	if cfg.Host == "" {
		cfg.Host, err = os.Hostname()
		Must(err, "failed to determine hostname")
	}

	var endpoint string
	switch addr := srv.RawListener.Addr().(type) {
	case *net.TCPAddr:
		endpoint = fmt.Sprintf("http://%s:%d", cfg.Host, addr.Port)
	case *net.UnixAddr:
		endpoint = fmt.Sprintf("%s://%s%s", addr.Net, cfg.Host, addr.Name)
	}

	return protocol.ProcessSpec{
		Id:       protocol.ProcessSpec_ID{Zone: cfg.Zone, Suffix: cfg.ID},
		Endpoint: protocol.Endpoint(endpoint),
	}
}
