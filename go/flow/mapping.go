package flow

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/bits"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"github.com/minio/highwayhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

var createdPartitionsCounters = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_collection_partitions_created_total",
	Help: "The number of new collection partitions created",
}, []string{"collection"})

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
	ctx      context.Context // TODO(johnny): Fix gazette so this is passed on the Map call.
	jc       pb.JournalClient
	journals Journals
}

// NewMapper builds and returns a new Mapper.
func NewMapper(ctx context.Context, jc pb.JournalClient, journals Journals) Mapper {
	return Mapper{
		ctx:      ctx,
		jc:       jc,
		journals: journals,
	}
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

	var bufPtr = mappingBufferPool.Get().(*[]byte)
	logicalPrefix, hexKey, buf := m.logicalPrefixAndHexKey((*bufPtr)[:0], msg)
	*bufPtr = buf

	defer func() {
		mappingBufferPool.Put(bufPtr)
	}()

	for attempt := 0; true; attempt++ {
		// Pick a partition at the current Etcd |revision|.
		m.journals.Mu.RLock()
		var revision = m.journals.Header.Revision
		var picked = m.pickPartition(logicalPrefix, hexKey)
		m.journals.Mu.RUnlock()

		if picked != nil {
			// Partition already exists (the common case).
			return picked.Name, picked.LabelSet.ValueOf(labels.ContentType), nil
		}

		// Build and attempt to apply a new physical partition for this logical partition.
		var applySpec, err = BuildPartitionSpec(msg.Spec.PartitionTemplate,
			// Build runtime labels of this partition from encoded logical
			// partition values, and an initial single physical partition.
			labels.EncodePartitionLabels(
				msg.Spec.PartitionFields, msg.Partitions,
				// We're creating a single physical partition, which covers
				// the full range of keys in the logical partition.
				pb.MustLabelSet(
					labels.KeyBegin, labels.KeyBeginMin,
					labels.KeyEnd, labels.KeyEndMax,
				)))
		if err != nil {
			panic(err) // Cannot fail because KeyBegin is always set.
		}

		applyResponse, err := client.ApplyJournals(m.ctx, m.jc, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{
					Upsert:            applySpec,
					ExpectModRevision: 0,
				},
			},
		})
		var readThrough int64

		if applyResponse != nil && applyResponse.Status == pb.Status_ETCD_TRANSACTION_FAILED {
			// We lost a race to create this journal, and |err| is "ETCD_TRANSACTION_FAILED".
			// This is expected to happen very infrequently, when we race
			// another process to create the journal. If it happens repeatedly
			// in a tight loop, it's likely that there's a mis-configuration of
			// the Etcd broker keyspace prefix.

			// We know that |revision| is behind and that we must await a future
			// journal revision, but we don't know which one. We *cannot* use the
			// Etcd revision of |applyResponse| because we're only watching a
			// portion of the broker's keyspace, and revisions it returns may
			// reflect members or assignments we're not watching. Were we to await
			// them, we could block indefinitely.

			// So, we simply await the next revision, which is near certain to
			// include it but isn't guaranteed to do so. It's possible that we'll
			// loop again.
			readThrough = revision + 1

			log.WithFields(log.Fields{
				"err":         err,
				"attempt":     attempt,
				"journal":     applySpec.Name,
				"readThrough": readThrough,
			}).Warn("failed to create partition (will retry)")

		} else if err != nil {
			return "", "", fmt.Errorf("creating journal '%s': %w", applySpec.Name, err)
		} else {
			// On success, |applyResponse| always reference the revision of the
			// applied Etcd transaction, which is guaranteed to produce an update
			// into |m.Journals|.
			readThrough = applyResponse.Header.Etcd.Revision

			log.WithFields(log.Fields{
				"attempt":     attempt,
				"journal":     applySpec.Name,
				"readThrough": readThrough,
			}).Info("created partition")
			createdPartitionsCounters.WithLabelValues(msg.Spec.Collection.String()).Inc()
		}

		m.journals.Mu.RLock()
		err = m.journals.WaitForRevision(m.ctx, readThrough)
		m.journals.Mu.RUnlock()

		if err != nil {
			return "", "", fmt.Errorf("awaiting journal revision '%d': %w", readThrough, err)
		}
	}
	panic("not reached")
}

func (m *Mapper) logicalPrefixAndHexKey(b []byte, msg Mappable) (logicalPrefix []byte, hexKey []byte, buf []byte) {
	b = append(b, m.journals.Root...)
	b = append(b, allocator.ItemsPrefix...)
	b = append(b, msg.Spec.PartitionTemplate.Name...)
	b = append(b, '/')

	for i, field := range msg.Spec.PartitionFields {
		b = append(b, field...)
		b = append(b, '=')
		b = labels.EncodePartitionValue(b, msg.Partitions[i])
		b = append(b, '/')
	}
	var pivot = len(b)

	b = appendHex32(b, PackedKeyHash_HH64(msg.PackedKey))

	return b[:pivot], b[pivot:], b
}

// appendHex32 matches the padded hex encoding of labels.EncodeRange,
// but is much faster than Sprintf and avoids allocation.
func appendHex32(b []byte, n uint32) []byte {
	for pad := bits.LeadingZeros32(n|0xf) / 4; pad != 0; pad-- {
		b = append(b, '0')
	}
	return strconv.AppendUint(b, uint64(n), 16)
}

func (m *Mapper) pickPartition(logicalPrefix []byte, hexKey []byte) *pb.JournalSpec {
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
		var keyEnd = physical[i].Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec).LabelSet.ValueOf(labels.KeyEnd)
		return keyEnd >= string(hexKey)
	})

	if ind == len(physical) {
		return nil
	}

	var p = physical[ind].Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec)
	if p.LabelSet.ValueOf(labels.KeyBegin) <= string(hexKey) {
		return p
	}
	return nil
}

var mappingBufferPool = sync.Pool{
	New: func() interface{} {
		var buf = new([]byte)
		*buf = make([]byte, 256)
		return buf
	},
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

// PackedKeyHash_HH64 builds a packed key hash from the top 32-bits of a
// HighwayHash 64-bit checksum computed using a fixed key.
func PackedKeyHash_HH64(packedKey []byte) uint32 {
	return uint32(highwayhash.Sum64(packedKey, highwayHashKey) >> 32)
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
var highwayHashKey, _ = hex.DecodeString("ba737e89155238d47d8067c35aad4d25ecdd1c3488227e011ffa480c022bd3ba")
