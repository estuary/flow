package flow

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unsafe"

	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

// Mapper maps IndexedCombineResponse documents into a corresponding logical
// partition, creating that partition if it doesn't yet exist.
type Mapper struct {
	Ctx           context.Context
	JournalClient pb.JournalClient
	Journals      *keyspace.KeySpace
}

// FieldPointersForMapper returns JSON-pointers of fields which should be extracted
// and included in CombineRespones mapped through a Mapper of this CollectionSpec.
func FieldPointersForMapper(collection *pf.CollectionSpec) []string {
	var ptrs []string
	for _, field := range collection.PartitionFields {
		// TODO: we should probably not panic when there's a malformed message
		var projection = pf.GetProjectionByField(field, collection.Projections)
		if projection == nil {
			panic("CollectionSpec partitionFields names a field that does not have a projection")
		}
		ptrs = append(ptrs, projection.Ptr)
	}
	ptrs = append(ptrs, collection.KeyPtrs...)
	return ptrs
}

// Map the Mappable, which must be an IndexedCombineResponse, into a physical journal partition
// of the document's logical partition prefix. If no such journal exists, one is created.
func (m *Mapper) Map(mappable message.Mappable) (pb.Journal, string, error) {
	var cr = mappable.(pf.IndexedCombineResponse)

	var buf = mappingBufferPool.Get().([]byte)[:0]
	logicalPrefix, hexKey, buf := m.logicalPrefixAndHexKey(buf, cr)

	defer func() {
		mappingBufferPool.Put(buf)
	}()

	for i := 0; true; i++ {
		m.Journals.Mu.RLock()
		var p = m.pickPartition(logicalPrefix, hexKey)
		m.Journals.Mu.RUnlock()

		if p != nil {
			// Partition already exists (the common case).
			return p.Name, p.LabelSet.ValueOf(labels.ContentType), nil
		}

		// We must create a new partition for this logical prefix.
		var upsert = m.partitionUpsert(cr)
		var applyResponse, err = client.ApplyJournals(m.Ctx, m.JournalClient, upsert)

		if applyResponse != nil && applyResponse.Status == pb.Status_ETCD_TRANSACTION_FAILED && i == 0 {
			// We lost a race to create this journal, and |err| is "ETCD_TRANSACTION_FAILED".
			// Ignore on the first attempt (only). If we see failures beyond that,
			// there's likely a mis-configuration of the Etcd broker keyspace prefix.
			continue
		} else if err != nil {
			return "", "", fmt.Errorf("creating journal '%s': %w", upsert.Changes[0].Upsert.Name, err)
		}

		// We applied the journal creation. Now wait to read it's Etcd watch update.
		m.Journals.Mu.RLock()
		err = m.Journals.WaitForRevision(m.Ctx, applyResponse.Header.Etcd.Revision)
		m.Journals.Mu.RUnlock()

		if err != nil {
			return "", "", fmt.Errorf("awaiting applied revision '%d': %w", applyResponse.Header.Etcd.Revision, err)
		}
		log.WithField("journal", upsert.Changes[0].Upsert.Name).Info("created partition")
	}
	panic("not reached")
}

func (m *Mapper) partitionUpsert(cr pf.IndexedCombineResponse) *pb.ApplyRequest {
	var spec = new(pb.JournalSpec)
	*spec = cr.Collection.JournalSpec

	spec.LabelSet.SetValue(flowLabels.Collection, cr.Collection.Name.String())
	spec.LabelSet.SetValue(flowLabels.KeyBegin, "00")
	spec.LabelSet.SetValue(flowLabels.KeyEnd, "ffffffff")
	spec.LabelSet.SetValue(labels.ContentType, labels.ContentType_JSONLines)

	var name strings.Builder
	name.WriteString(cr.Collection.Name.String())

	for i, field := range cr.Collection.PartitionFields {
		var v = cr.Fields[i].Values[cr.Index].EncodePartition(nil, cr.Arena)
		spec.LabelSet.AddValue(flowLabels.FieldPrefix+field, string(v))

		name.WriteByte('/')
		name.WriteString(field)
		name.WriteByte('=')
		name.Write(v)
	}
	name.WriteString("/pivot=00")
	spec.Name = pb.Journal(name.String())

	return &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				Upsert:            spec,
				ExpectModRevision: 0,
			},
		},
	}
}

func (m *Mapper) logicalPrefixAndHexKey(b []byte, cr pf.IndexedCombineResponse) (logicalPrefix []byte, hexKey []byte, buf []byte) {
	b = append(b, m.Journals.Root...)
	b = append(b, '/')
	b = append(b, cr.Collection.Name...)
	b = append(b, '/')

	for i, field := range cr.Collection.PartitionFields {
		b = append(b, field...)
		b = append(b, '=')
		b = cr.Fields[i].Values[cr.Index].EncodePartition(b, cr.Arena)
		b = append(b, '/')
	}
	var pivot = len(b)

	// Extract remaining fields _after_ |partitions| -- which are the composite collection key --
	// into a packed and hex-encoded representation that matches how journals are labeled with key ranges.
	const hextable = "0123456789abcdef"
	var scratch [64]byte

	for _, field := range cr.Fields[len(cr.Collection.PartitionFields):] {
		for _, v := range field.Values[cr.Index].EncodePacked(scratch[:0], cr.Arena) {
			b = append(b, hextable[v>>4], hextable[v&0x0f])
		}
	}
	return b[:pivot], b[pivot:], b
}

func (m *Mapper) pickPartition(logicalPrefix []byte, hexKey []byte) *pb.JournalSpec {
	// This unsafe cast avoids |logicalPrefix| escaping to heap, as would otherwise
	// happen due to it's use within a closure that crosses the sort.Search interface
	// boundary. It's safe to do because the value is not retained or used beyond
	// the journals.Prefixed call.
	var logicalPrefixStrUnsafe = *(*string)(unsafe.Pointer(&logicalPrefix))
	// Map |logicalPrefix| into a set of physical partitions.
	var physical = m.Journals.Prefixed(logicalPrefixStrUnsafe)

	// Find the first physical partition having KeyEnd > hexKey.
	// Note we're performing this comparasion in a hex-encoded space.
	var ind = sort.Search(len(physical), func(i int) bool {
		var keyEnd = physical[i].Decoded.(*pb.JournalSpec).LabelSet.ValueOf(flowLabels.KeyEnd)
		return keyEnd > string(hexKey)
	})

	if ind == len(physical) {
		return nil
	} else if p := physical[ind].Decoded.(*pb.JournalSpec); p.LabelSet.ValueOf(flowLabels.KeyBegin) <= string(hexKey) {
		return p
	}
	return nil
}

var mappingBufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 256) },
}
