package network

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// Parsed portions of the TLS ServerName which are used to map to a shard.
type parsedSNI struct {
	hostname    string
	port        string
	keyBegin    string
	rClockBegin string
}

// Resolved task shard metadata which allow us to complete TLS handshake.
type resolvedSNI struct {
	portIsPublic  bool
	portProtocol  string
	shardIDPrefix string
	taskName      string
}

// parseSNI parses a `target` into a parsedSNI.
// We accept two forms of targets:
// * d7f4a9d02b48c1a-6789 (hostname and port)
// * d7f4a9d02b48c1a-00000000-80000000-6789 (hostname, key begin, r-clock begin, and port).
func parseSNI(target string) (parsedSNI, error) {
	var parts = strings.Split(target, "-")
	var hostname, port, keyBegin, rClockBegin string

	if len(parts) == 2 {
		hostname = parts[0]
		port = parts[1]
	} else if len(parts) == 4 {
		hostname = parts[0]
		keyBegin = parts[1]
		rClockBegin = parts[2]
		port = parts[3]
	} else {
		return parsedSNI{}, fmt.Errorf("expected two or for subdomain components, not %d", len(parts))
	}

	var _, err = strconv.ParseUint(port, 10, 16)
	if err != nil {
		return parsedSNI{}, fmt.Errorf("failed to parse subdomain port number: %w", err)
	}

	return parsedSNI{
		hostname:    hostname,
		port:        port,
		keyBegin:    keyBegin,
		rClockBegin: rClockBegin,
	}, nil
}

func newResolvedSNI(parsed parsedSNI, shard *pc.ShardSpec) resolvedSNI {
	var shardIDPrefix = shard.Id.String()

	// Strip final Shard ID suffix, like `00000000-00000000`.
	if ind := strings.LastIndexByte(shardIDPrefix, '/'); ind != -1 {
		shardIDPrefix = shardIDPrefix[:ind]
	}
	// Strip embedded creation publication ID, like `0123457890abcdef`.
	// If we didn't do this, a deletion and creation of a task with the
	// same name would break our resolution index cache.
	if ind := strings.LastIndexByte(shardIDPrefix, '/'); ind != -1 {
		shardIDPrefix = shardIDPrefix[:ind+1] // Retain trailing '/'.
	}

	var portProtocol = shard.LabelSet.ValueOf(labels.PortProtoPrefix + parsed.port)
	var portIsPublic = shard.LabelSet.ValueOf(labels.PortPublicPrefix+parsed.port) == "true"

	// HTTP/1.1 is the only protocol which we reverse proxy. It's the assumed
	// protocol if none is specified, and is required if the port is private.
	if portProtocol == "" || !portIsPublic {
		portProtocol = protoHTTP11
	} else if portProtocol == "h2c" {
		// Connector expects cleartext HTTP/2. We terminate TLS and TCP proxy.
		portProtocol = protoHTTP2
	}

	return resolvedSNI{
		shardIDPrefix: shardIDPrefix,
		portProtocol:  portProtocol,
		portIsPublic:  portIsPublic,
		taskName:      shard.LabelSet.ValueOf(labels.TaskName),
	}
}

func listShards(ctx context.Context, shards pc.ShardClient, parsed parsedSNI, shardIDPrefix string) ([]pc.ListResponse_Shard, error) {
	var include = []pb.Label{
		{Name: labels.ExposePort, Value: parsed.port},
		{Name: labels.Hostname, Value: parsed.hostname},
	}
	if parsed.keyBegin != "" {
		include = append(include, pb.Label{Name: labels.KeyBegin, Value: parsed.keyBegin})
	}
	if parsed.rClockBegin != "" {
		include = append(include, pb.Label{Name: labels.RClockBegin, Value: parsed.rClockBegin})
	}
	if shardIDPrefix != "" {
		include = append(include, pb.Label{Name: "id", Value: shardIDPrefix, Prefix: true})
	}

	var resp, err = shards.List(
		pb.WithDispatchDefault(ctx),
		&pc.ListRequest{
			Selector: pb.LabelSelector{Include: pb.LabelSet{Labels: include}},
		},
	)
	if err == nil && resp.Status != pc.Status_OK {
		err = errors.New(resp.Status.String())
	}
	if err != nil {
		return nil, pf.UnwrapGRPCError(err)
	}

	return resp.Shards, nil
}

const (
	protoHTTP11 = "http/1.1"
	protoHTTP2  = "h2"
)
