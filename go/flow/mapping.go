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

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/minio/highwayhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
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
	List       *client.WatchedList
}

var _ message.Message = Mappable{}

// Mapper maps IndexedCombineResponse documents into a corresponding logical
// partition, creating that partition if it doesn't yet exist.
type Mapper struct {
	ctx context.Context
	jc  pb.JournalClient
}

// NewMapper builds and returns a new Mapper, which monitors from |journals|
// and creates new partitions into the given |etcd| client.
func NewMapper(ctx context.Context, jc pb.JournalClient) Mapper {
	return Mapper{ctx: ctx, jc: jc}
}

// Map |mappable|, which must be an instance of Mappable, into a physical journal partition
// of the document's logical partition prefix. If no such journal exists, one is created.
func (m *Mapper) Map(mappable message.Mappable) (pb.Journal, string, error) {
	var msg = mappable.(Mappable)

	var bufPtr = mappingBufferPool.Get().(*[]byte)
	var logicalPrefix, hexKey, buf = logicalPrefixAndHexKey((*bufPtr)[:0], msg)
	*bufPtr = buf

	defer func() {
		mappingBufferPool.Put(bufPtr)
	}()

	for attempt := 0; true; attempt++ {
		// Pick an available partition.
		var picked = pickPartition(logicalPrefix, hexKey, msg.List.List().Journals)

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

		var ctx = pb.WithClaims(pb.WithDispatchDefault(m.ctx), pb.Claims{
			Capability: pb.Capability_APPLY,
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("name", applySpec.Name.String()),
			},
		})
		resp, err := m.jc.Apply(ctx, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{
					Upsert:            applySpec,
					ExpectModRevision: 0, // Expect it's created by this Apply.
				},
			},
		})

		if err != nil {
			return "", "", fmt.Errorf("creating partition %s: %w", applySpec.Name, err)
		} else if resp.Status == pb.Status_ETCD_TRANSACTION_FAILED {
			// We lost a race to create this journal.
			//
			// This is expected to happen very infrequently, when we race
			// another process to create the journal, or are racing the removal
			// of the shard spec under which we're running.
			log.WithFields(log.Fields{
				"attempt":     attempt,
				"journal":     applySpec.Name,
				"readThrough": resp.Header.Etcd.Revision,
			}).Info("lost race to create partition")
		} else if resp.Status == pb.Status_OK {
			log.WithFields(log.Fields{
				"attempt":     attempt,
				"journal":     applySpec.Name,
				"readThrough": resp.Header.Etcd.Revision,
			}).Info("created partition")
			createdPartitionsCounters.WithLabelValues(msg.Spec.Name.String()).Inc()
		} else {
			return "", "", fmt.Errorf("creating partition %s: %s", applySpec.Name, resp.Status)
		}

		// Wait for a listing update notification.
		select {
		case <-msg.List.UpdateCh():
		case <-m.ctx.Done():
		}
	}
	panic("not reached")
}

func logicalPrefixAndHexKey(b []byte, msg Mappable) (logicalPrefix []byte, hexKey []byte, buf []byte) {
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

func pickPartition(logicalPrefix []byte, hexKey []byte, journals []pb.ListResponse_Journal) *pb.JournalSpec {
	// Find the first physical partition having `logicalPrefix` as its Name prefix
	// and label KeyEnd > `hexKey`. Note we're performing the latter comparison in
	// a hex-encoded space.
	var ind = sort.Search(len(journals), func(i int) bool {
		var pre = journals[i].Spec.Name

		if l, r := len(logicalPrefix), len(pre); l < r {
			pre = pre[:l] // Compare over the prefix.
		}
		switch bytes.Compare([]byte(pre), logicalPrefix) {
		case -1:
			return false
		case 0:
			// Prefixes match. Now compare over KeyEnd.
			return journals[i].Spec.LabelSet.ValueOf(labels.KeyEnd) >= string(hexKey)
		case 1:
			return true
		default:
			panic("not reached")
		}
	})

	if ind == len(journals) {
		return nil
	} else if p := &journals[ind].Spec; len(p.Name) < len(logicalPrefix) {
		return nil
	} else if !bytes.Equal([]byte(p.Name[:len(logicalPrefix)]), logicalPrefix) {
		return nil
	} else if p.LabelSet.ValueOf(labels.KeyBegin) > string(hexKey) {
		return nil
	} else {
		return p
	}
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

// NewAcknowledgementMessage returns a new acknowledgement message for a journal of the given
// collection.
func NewAcknowledgementMessage(spec *pf.CollectionSpec) Mappable {
	return Mappable{
		Spec: spec,
		Doc:  append(json.RawMessage(nil), spec.AckTemplateJson...),
	}
}

// NewAcknowledgement returns an Mappable of the acknowledgement template.
func (m Mappable) NewAcknowledgement(pb.Journal) message.Message {
	return NewAcknowledgementMessage(m.Spec)
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
