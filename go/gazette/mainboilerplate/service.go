package mainboilerplate

import (
	"net/http"

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
	ID                string   `long:"id" env:"ID" description:"Unique ID of this process. Auto-generated if not set"`
	Host              string   `long:"host" env:"HOST" description:"Addressable, advertised hostname or IP of this process. Hostname is used if not set"`
	Port              string   `long:"port" env:"PORT" description:"Service port for HTTP and gRPC requests. A random port is used if not set. Port may also take the form 'unix:///path/to/socket' to use a Unix Domain Socket"`
	ServerCertFile    string   `long:"server-cert-file" env:"SERVER_CERT_FILE" default:"" description:"Path to the server TLS certificate. This option toggles whether TLS is used. If absent, all other TLS settings are ignored."`
	ServerCertKeyFile string   `long:"server-cert-key-file" env:"SERVER_CERT_KEY_FILE" default:"" description:"Path to the server TLS private key"`
	ServerCAFile      string   `long:"server-ca-file" env:"SERVER_CA_FILE" default:"" description:"Path to the trusted CA for server verification of client certificates. When present, client certificates are required and verified against this CA. When absent, client certificates are not required but are verified against the system CA pool if presented."`
	PeerCertFile      string   `long:"peer-cert-file" env:"PEER_CERT_FILE" default:"" description:"Path to the client TLS certificate for peer-to-peer requests"`
	PeerCertKeyFile   string   `long:"peer-cert-key-file" env:"PEER_CERT_KEY_FILE" default:"" description:"Path to the client TLS private key for peer-to-peer requests"`
	PeerCAFile        string   `long:"peer-ca-file" env:"PEER_CA_FILE" default:"" description:"Path to the trusted CA for client verification of peer server certificates. When absent, the system CA pool is used instead."`
	MaxGRPCRecvSize   uint32   `long:"max-grpc-recv-size" env:"MAX_GRPC_RECV_SIZE" default:"4194304" description:"Maximum size of gRPC messages accepted by this server, in bytes"`
	AllowOrigin       []string `long:"allow-origin" env:"ALLOW_ORIGIN" description:"Origin to allow in CORS contexts"`
}

// ProcessSpec of the ServiceConfig.
func (cfg ServiceConfig) BuildProcessSpec(srv *server.Server) protocol.ProcessSpec {
	if cfg.ID == "" {
		cfg.ID = petname.Generate(2, "-")
	}

	return protocol.ProcessSpec{
		Id:       protocol.ProcessSpec_ID{Zone: cfg.Zone, Suffix: cfg.ID},
		Endpoint: srv.Endpoint(),
	}
}

func (cfg ServiceConfig) CORSWrapper(wrapped http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var origin = r.Header.Get("origin")

		for _, match := range cfg.AllowOrigin {
			if origin == match {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Cache-Control, Content-Language, Content-Length, Content-Type, Expires, Last-Modified, Pragma, Authorization")
			}
		}

		if r.Method == "OPTIONS" {
			return
		}
		wrapped.ServeHTTP(w, r.WithContext(protocol.WithDispatchDefault(r.Context())))
	})
}
