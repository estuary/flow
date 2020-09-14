package flow

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"unsafe"

	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

// Mapping maps IndexedCombineResponse documents into a corresponding logical
// partition, creating that partition if it doesn't yet exist.
type Mapping struct {
	ctx        context.Context
	rjc        pb.RoutedJournalClient
	collection pf.Collection
	partitions []string
	model      pb.JournalSpec
	journals   *keyspace.KeySpace
}

// Map the Mappable, which must be an IndexedCombineResponse, into a physical journal partition
// of the document's logical partition prefix. If no such journal exists, one is created.
func (m *Mapping) Map(mappable message.Mappable) (pb.Journal, string, error) {
	var cr = mappable.(pf.IndexedCombineResponse)

	var buf = mappingBufferPool.Get().([]byte)[:0]
	logicalPrefix, hexKey, buf := m.logicalPrefixAndHexKey(buf, cr)

	m.journals.Mu.RLock()
	defer func() {
		m.journals.Mu.RUnlock()
		mappingBufferPool.Put(buf)
	}()

	for {
		if p := m.pickPartition(logicalPrefix, hexKey); p != nil {
			return p.Name, p.LabelSet.ValueOf(labels.ContentType), nil
		}
		// We must create a new partition for this logical prefix.
		var upsert = m.partitionUpsert(cr)
		var applyResponse, err = client.ApplyJournals(m.ctx, m.rjc, upsert)

		if applyResponse != nil && applyResponse.Status == pb.Status_ETCD_TRANSACTION_FAILED {
			// We lost a race to create this journal. Ignore.
		} else if err != nil {
			return "", "", fmt.Errorf("creating journal '%s': %w", upsert.Changes[0].Upsert.Name, err)
		} else if err = m.journals.WaitForRevision(m.ctx, applyResponse.Header.Etcd.Revision); err != nil {
			return "", "", fmt.Errorf("awaiting applied revision '%d': %w", applyResponse.Header.Etcd.Revision, err)
		}
	}
}

func (m *Mapping) partitionUpsert(cr pf.IndexedCombineResponse) *pb.ApplyRequest {
	var spec = new(pb.JournalSpec)
	*spec = m.model

	spec.LabelSet.AddValue(flowLabels.Collection, m.collection.String())
	spec.LabelSet.AddValue(flowLabels.KeyBegin, "")
	spec.LabelSet.AddValue(flowLabels.KeyEnd, "ffffffff")
	spec.LabelSet.AddValue(labels.ContentType, labels.ContentType_JSONLines)

	var name strings.Builder
	name.WriteString(m.collection.String())

	for i, partition := range m.partitions {
		var (
			k = url.PathEscape(partition)
			v = url.PathEscape(cr.Fields[i].Values[cr.Index].ToJSON(cr.Arena))
		)
		spec.LabelSet.AddValue(flowLabels.FieldPrefix+k, v)

		name.WriteByte('/')
		name.WriteString(k)
		name.WriteByte('=')
		name.WriteString(v)
	}
	name.WriteString("/_phys=0000")
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

func (m *Mapping) logicalPrefixAndHexKey(b []byte, cr pf.IndexedCombineResponse) (logicalPrefix []byte, hexKey []byte, buf []byte) {
	b = append(b, m.journals.Root...)
	b = append(b, '/')
	b = append(b, m.collection...)
	b = append(b, '/')

	for i, partition := range m.partitions {
		b = append(b, url.PathEscape(partition)...)
		b = append(b, '=')
		b = append(b,
			url.PathEscape(
				cr.Fields[i].Values[cr.Index].ToJSON(cr.Arena))...)
		b = append(b, '/')
	}
	var pivot = len(b)

	// Extract remaining fields _after_ |partitions| -- which are the composite collection key --
	// into a packed and hex-encoded representation that matches how journals are labeled with key ranges.
	const hextable = "0123456789abcdef"
	var scratch [64]byte

	for _, field := range cr.Fields[len(m.partitions):] {
		for _, v := range field.Values[cr.Index].EncodePacked(scratch[:0], cr.Arena) {
			b = append(b, hextable[v>>4], hextable[v&0x0f])
		}
	}
	return b[:pivot], b[pivot:], b
}

func (m *Mapping) pickPartition(logicalPrefix []byte, hexKey []byte) *pb.JournalSpec {
	// This unsafe cast avoids |logicalPrefix| escaping to heap, as would otherwise
	// happen due to it's use within a closure that crosses the sort.Search interface
	// boundary. It's safe to do because the value is not retained or used beyond
	// the journals.Prefixed call.
	var logicalPrefixStrUnsafe = *(*string)(unsafe.Pointer(&logicalPrefix))
	// Map |logicalPrefix| into a set of physical partitions.
	var physical = m.journals.Prefixed(logicalPrefixStrUnsafe)

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
