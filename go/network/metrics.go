package network

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var handshakeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_handshake_total",
	Help: "counter of connections which attempted TLS handshake with the connector network proxy frontend",
}, []string{"status"})

var userStartedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_user_started_total",
	Help: "counter of started user-initiated connections to the connector network proxy frontend",
}, []string{"task", "port", "proto"})

var userHandledCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_user_handled_total",
	Help: "counter of handled user-initiated connections to the connector network proxy frontend",
}, []string{"task", "port", "proto", "status"})

var shardStartedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_shard_started_total",
	Help: "counter of started shard connector client connections initiated by the network proxy",
}, []string{"task", "port", "proto"})

var shardHandledCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_shard_handled_total",
	Help: "counter of handled shard connector client connections initiated by the network proxy",
}, []string{"task", "port", "proto", "status"})

var httpStartedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_http_started_total",
	Help: "counter of started reverse-proxy connector HTTP requests initiated by the network proxy",
}, []string{"task", "port", "method"})

var httpHandledCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_http_handled_total",
	Help: "counter of handled reverse-proxy connector HTTP requests initiated by the network proxy",
}, []string{"task", "port", "status"})

var bytesReceivedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_bytes_received_total",
	Help: "counter of bytes received from user connections by the connector network proxy frontend",
}, []string{"task", "port", "proto"})

var bytesSentCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_net_proxy_bytes_sent_total",
	Help: "counter of bytes sent to user connections by the connector network proxy frontend",
}, []string{"task", "port", "proto"})
