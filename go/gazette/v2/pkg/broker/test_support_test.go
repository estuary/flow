package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	gc "github.com/go-check/check"
)

type testBroker struct {
	id pb.ProcessSpec_ID
	*teststub.Server

	*teststub.Broker // nil if not built with newMockBroker.
	*resolver        // nil if not built with newTestBroker.
}

func newTestBroker(c *gc.C, ctx context.Context, ks *keyspace.KeySpace, id pb.ProcessSpec_ID) *testBroker {
	var state = allocator.NewObservedState(ks, allocator.MemberKey(ks, id.Zone, id.Suffix))
	var res = newResolver(state, newReplica)

	var svc = &Service{resolver: res}
	var srv = teststub.NewServer(c, ctx, svc)
	svc.jc = srv.MustClient()

	c.Check(ks.Apply(etcdEvent(ks, "put", state.LocalKey, (&pb.BrokerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       id,
			Endpoint: srv.Endpoint(),
		},
	}).MarshalString())), gc.IsNil)

	return &testBroker{
		id:       id,
		Server:   srv,
		resolver: res,
	}
}

func newMockBroker(c *gc.C, ctx context.Context, ks *keyspace.KeySpace, id pb.ProcessSpec_ID) *testBroker {
	var broker = teststub.NewBroker(c, ctx)
	var key = allocator.MemberKey(ks, id.Zone, id.Suffix)

	c.Check(ks.Apply(etcdEvent(ks, "put", key, (&pb.BrokerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       id,
			Endpoint: broker.Server.Endpoint(),
		},
	}).MarshalString())), gc.IsNil)

	return &testBroker{
		id:     id,
		Server: broker.Server,
		Broker: broker,
	}
}

func newTestJournal(c *gc.C, ks *keyspace.KeySpace, journal pb.Journal, replication int32, ids ...pb.ProcessSpec_ID) {
	var tkv []string

	// Create JournalSpec.
	tkv = append(tkv, "put",
		allocator.ItemKey(ks, journal.String()),
		(&pb.JournalSpec{
			Name:        journal,
			Replication: replication,
			Fragment: pb.JournalSpec_Fragment{
				Length:           1024,
				RefreshInterval:  time.Second,
				CompressionCodec: pb.CompressionCodec_SNAPPY,
			},
		}).MarshalString())

	// Create broker assignments.
	for slot, id := range ids {
		if id == (pb.ProcessSpec_ID{}) {
			continue
		}

		var key = allocator.AssignmentKey(ks, allocator.Assignment{
			ItemID:       journal.String(),
			MemberZone:   id.Zone,
			MemberSuffix: id.Suffix,
			Slot:         slot,
		})

		tkv = append(tkv, "put", key, "")
	}
	c.Check(ks.Apply(etcdEvent(ks, tkv...)), gc.IsNil)
}

func etcdEvent(ks *keyspace.KeySpace, typeKeyValue ...string) clientv3.WatchResponse {
	if len(typeKeyValue)%3 != 0 {
		panic("not type/key/value")
	}

	defer ks.Mu.RUnlock()
	ks.Mu.RLock()

	var resp = clientv3.WatchResponse{
		Header: etcdserverpb.ResponseHeader{
			ClusterId: 0xfeedbeef,
			MemberId:  0x01234567,
			RaftTerm:  0x11223344,
			Revision:  ks.Header.Revision + 1,
		},
	}

	for i := 0; i != len(typeKeyValue); i += 3 {
		var typ, key, value = typeKeyValue[i], typeKeyValue[i+1], typeKeyValue[i+2]

		var event = &clientv3.Event{
			Kv: &mvccpb.KeyValue{
				Key:         []byte(key),
				Value:       []byte(value),
				ModRevision: ks.Header.Revision + 1,
			},
		}
		var ind, ok = ks.Search(key)

		switch typ {
		case "put":
			event.Type = clientv3.EventTypePut
		case "del":
			event.Type = clientv3.EventTypeDelete
			if !ok {
				panic("!ok")
			}
		default:
			panic(typ)
		}

		if ok {
			var cur = ks.KeyValues[ind].Raw
			event.Kv.CreateRevision = cur.CreateRevision
			event.Kv.Version = cur.Version + 1
			event.Kv.Lease = cur.Lease
		} else {
			event.Kv.CreateRevision = event.Kv.ModRevision
			event.Kv.Version = 1
		}

		resp.Events = append(resp.Events, event)
	}
	return resp
}
