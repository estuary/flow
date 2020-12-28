package flow

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/estuary/flow/go/fdb/tuple"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

// Mappable is the implementation of message.Message which is expected by Mapper.
type Mappable struct {
	Spec       *pf.CollectionSpec
	Doc        json.RawMessage
	PackedKey  []byte
	Partitions tuple.Tuple
}

var _ message.Message = Mappable{}

// Mapper maps IndexedCombineResponse documents into a corresponding logical
// partition, creating that partition if it doesn't yet exist.
type Mapper struct {
	Ctx           context.Context
	JournalClient pb.JournalClient
	Journals      *keyspace.KeySpace
}

// PartitionPointers returns JSON-pointers of partitioned fields of the collection.
func PartitionPointers(spec *pf.CollectionSpec) []string {
	var ptrs = make([]string, len(spec.PartitionFields))
	for i, field := range spec.PartitionFields {
		ptrs[i] = pf.GetProjectionByField(field, spec.Projections).Ptr
	}
	return ptrs
}

// Map |mappable|, which must be an instance of Mappable, into a physical journal partition
// of the document's logical partition prefix. If no such journal exists, one is created.
func (m *Mapper) Map(mappable message.Mappable) (pb.Journal, string, error) {
	var msg = mappable.(Mappable)

	var buf = mappingBufferPool.Get().([]byte)[:0]
	logicalPrefix, hexKey, buf := m.logicalPrefixAndHexKey(buf, msg)

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
		var upsert = m.partitionUpsert(msg)
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

func (m *Mapper) partitionUpsert(msg Mappable) *pb.ApplyRequest {
	var spec = new(pb.JournalSpec)
	*spec = msg.Spec.JournalSpec

	spec.LabelSet.SetValue(flowLabels.Collection, msg.Spec.Name.String())
	spec.LabelSet.SetValue(flowLabels.KeyBegin, "00")
	spec.LabelSet.SetValue(flowLabels.KeyEnd, "ffffffff")
	spec.LabelSet.SetValue(labels.ContentType, labels.ContentType_JSONLines)

	var name strings.Builder
	name.WriteString(msg.Spec.Name.String())

	for i, field := range msg.Spec.PartitionFields {
		var v = encodePartitionElement(nil, msg.Partitions[i])
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

func (m *Mapper) logicalPrefixAndHexKey(b []byte, msg Mappable) (logicalPrefix []byte, hexKey []byte, buf []byte) {
	b = append(b, m.Journals.Root...)
	b = append(b, '/')
	b = append(b, msg.Spec.Name...)
	b = append(b, '/')

	for i, field := range msg.Spec.PartitionFields {
		b = append(b, field...)
		b = append(b, '=')
		b = encodePartitionElement(b, msg.Partitions[i])
		b = append(b, '/')
	}
	var pivot = len(b)

	// Hex-encode the packed key representation into |b|.
	const hextable = "0123456789abcdef"

	for _, v := range msg.PackedKey {
		b = append(b, hextable[v>>4], hextable[v&0x0f])
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

func encodePartitionElement(b []byte, elem tuple.TupleElement) []byte {
	switch v := elem.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		if v {
			return append(b, "true"...)
		} else {
			return append(b, "false"...)
		}
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case string:
		return append(b, url.PathEscape(v)...)
	default:
		panic(fmt.Sprintf("invalid element type: %#v", elem))
	}
}

// Implementation of message.Message for Mappable follows:

// GetUUID panics if called.
func (m Mappable) GetUUID() message.UUID { panic("not implemented") }

// SetUUID replaces the placeholder UUID string, which must exist, with the UUID.
func (m Mappable) SetUUID(uuid message.UUID) {
	// Require that the current content has a placeholder UUID.
	var ind = bytes.Index(m.Doc, pf.DocumentUUIDPlaceholder)
	if ind == -1 {
		panic("document UUID placeholder not found")
	}

	// Replace it with the string-form UUID.
	var str = uuid.String()
	copy(m.Doc[ind:ind+36], str[0:36])
}

// NewAcknowledgement returns an Mappable of the acknowledgement template.
func (m Mappable) NewAcknowledgement(pb.Journal) message.Message {
	return Mappable{
		Spec: m.Spec,
		Doc:  append(json.RawMessage(nil), m.Spec.AckJsonTemplate...),
	}
}

// MarshalJSONTo copies the raw document json into the Writer.
func (m Mappable) MarshalJSONTo(bw *bufio.Writer) (int, error) {
	var n, _ = bw.Write(m.Doc)
	return n + 1, bw.WriteByte('\n')
}
