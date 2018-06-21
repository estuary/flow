package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/util/encoding"
	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/LiveRamp/gazette/pkg/consensus"
	"github.com/LiveRamp/gazette/pkg/journal"
	"github.com/LiveRamp/gazette/pkg/metrics"
	"github.com/LiveRamp/gazette/pkg/recoverylog"
	"github.com/LiveRamp/gazette/pkg/topic"
)

// Paired routines for loading/storing/working with hints, offsets, and
// shard <=> journal mappings. These are extracted from and unit-tested
// independently of their uses in `master` and `replica`.

const (
	// Etcd directory into which FSM hints are stored.
	hintsPrefix = "hints"
	// Legacy Etcd offsets path.
	offsetsPrefix   = "offsets"
	validGroupChars = "abcdefghijklmnopqrstuvwxyz0123456789-"
)

// Generates a ShardID for a topic.Partition. Shards are named as:
//  "shard-{path.Base(topic.Name)}-{Base(partition)}". As a special case, if the
// partition was created using standard "my-topic/part-123" enumeration, the
// "part-" prefix of the journal base name is removed resulting in final ShardIDs
// like "shard-my-topic-123".
//
// This method is deprecated, but maintained for compatibility with existing
// ShardIDs used in production.
// TODO(johnny): Move to a globally unique ShardID, which is content-addressed
// from the (Consumer, Topic, Journal)-tuple names.
func ShardName_DEPRECATED(p topic.Partition) ShardID {
	var group = path.Base(p.Topic.Name)
	var name = path.Base(p.Journal.String())

	// Remove standard prefix for Journals created with topic.EnumeratePartitions.
	name = strings.TrimPrefix(name, "part-")

	return ShardID(fmt.Sprintf("shard-%s-%s", group, name))
}

// EnumerateShards returns a mapping of unique ShardIDs and their Partitions
// implied by the Consumer and its set of consumed Topics.
func EnumerateShards(c Consumer) map[ShardID]topic.Partition {
	var m = make(map[ShardID]topic.Partition)

	for _, t := range c.Topics() {
		for _, j := range t.Partitions() {
			var p = topic.Partition{Topic: t, Journal: j}
			// TODO(johnny): Move to a content-addressed global shard ID, derived from
			// consumer, topic, and partition names.
			var shardID = ShardName_DEPRECATED(p)

			m[shardID] = p
		}
	}
	return m
}

// Maps a consumer |tree| and |shard| to the full path for storing FSMHints.
// Eg, hintsPath(tree{/a/consumer}, 42) => "/a/consumer/hints/shard-042".
func hintsPath(consumerPath string, shard ShardID) string {
	return consumerPath + "/" + hintsPrefix + "/" + shard.String()
}

func OffsetPath(consumerPath string, name journal.Name) string {
	return consumerPath + "/" + offsetsPrefix + "/" + name.String()
}

// Maps |shard| to its recovery log journal.
func recoveryLog(logRoot string, shard ShardID) journal.Name {
	return journal.Name(path.Join(logRoot, shard.String()))
}

// Aborts processing of |shard| by |runner|. Called only in exceptional
// circumstances (eg, an unrecoverable local error).
func abort(runner *Runner, shard ShardID) {
	if err := consensus.CancelItem(runner, shard.String()); err != nil {
		metrics.GazetteConsumerFailedShardLocksTotal.Inc()
	}
}

// Loads JSON-encoded FSMHints from |tree| for |shard|. If hints do not exist,
// initializes new hints using the default RecoveryLogRoot root.
func loadHintsFromEtcd(shard ShardID, runner *Runner, tree *etcd.Node) (recoverylog.FSMHints, error) {
	var hints recoverylog.FSMHints
	var key = hintsPath(tree.Key, shard)
	var parent, i = consensus.FindNode(tree, key)

	if i < len(parent.Nodes) && parent.Nodes[i].Key == key {
		if err := json.Unmarshal([]byte(parent.Nodes[i].Value), &hints); err != nil {
			return recoverylog.FSMHints{}, err
		}
	}

	if hints.Log == "" {
		hints.Log = recoveryLog(runner.RecoveryLogRoot, shard)
	}
	return hints, nil
}

// hintsJSONString returns the JSON string encoding of |hints|.
func hintsJSONString(hints recoverylog.FSMHints) string {
	if b, err := json.Marshal(hints); err != nil {
		panic(err.Error()) // JSON serialization of FSMHints can never fail.
	} else {
		return string(b)
	}
}

// maybeEtcdSet attempts to set |key| to |value|, consuming an encountered error
// by logging a warning. This routine simplifies usages making best-effort attempts
// to write to Etcd which are permitted to fail.
func maybeEtcdSet(keysAPI etcd.KeysAPI, key string, value string) error {
	var _, err = keysAPI.Set(context.Background(), key, value, nil)
	// Etcd Set is best-effort.
	if err != nil {
		log.WithFields(log.Fields{"key": key, "err": err}).Warn("failed to set etcd key")
		return err
	}
	return nil
}

// Converts |offset| into a base-16 encoded string.
func offsetToString(offset int64) string {
	return strconv.FormatInt(offset, 16)
}

// Parses |str| as a base-16 encoded integer.
func offsetFromString(str string) (int64, error) {
	return strconv.ParseInt(str, 16, 64)
}

// Loads legacy offsets stored in Etcd under |tree|.
func LoadOffsetsFromEtcd(tree *etcd.Node) (map[journal.Name]int64, error) {
	node := consensus.Child(tree, offsetsPrefix)
	if node == nil {
		return nil, nil
	}

	var result = make(map[journal.Name]int64)
	for _, n := range consensus.TerminalNodes(node) {
		name := n.Key[len(node.Key)+1:]

		offset, err := offsetFromString(n.Value)
		if err != nil {
			return nil, err
		}
		result[journal.Name(name)] = offset
	}
	return result, nil
}

// Deprecated: We are removing support for offsets written to Etcd. Rocksdb
// will be the sole source-of-truth for read offsets.
// Stores legacy |offsets| in Etcd.
func StoreOffsetsToEtcd(rootPath string, offsets map[journal.Name]int64, keysAPI etcd.KeysAPI) {
	for name, offset := range offsets {
		maybeEtcdSet(keysAPI, OffsetPath(rootPath, name), strconv.FormatInt(offset, 16))
	}
}

// AppendOffsetKeyEncoding encodes |name| into a database key representing
// a consumer journal offset checkpoint. A |name| of "" will generate a
// key which prefixes all other offset key encodings.
func AppendOffsetKeyEncoding(b []byte, name journal.Name) []byte {
	b = encoding.EncodeNullAscending(b)
	b = encoding.EncodeStringAscending(b, "mark")
	if name != "" {
		b = encoding.EncodeStringAscending(b, string(name))
	}
	return b
}

// AppendOffsetValueEncoding encodes |offset| into a database value representing
// a consumer journal offset checkpoint.
func AppendOffsetValueEncoding(b []byte, offset int64) []byte {
	return encoding.EncodeVarintAscending(b, offset)
}

// Loads from |db| offsets previously serialized by storeAndClearOffsets.
func LoadOffsetsFromDB(db *rocks.DB, dbRO *rocks.ReadOptions) (map[journal.Name]int64, error) {
	var prefix = AppendOffsetKeyEncoding(nil, "")
	var result = make(map[journal.Name]int64)

	var it = db.NewIterator(dbRO)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var key, val = it.Key().Data(), it.Value().Data()

		var _, name, err1 = encoding.DecodeStringAscending(key[len(prefix):], nil)
		var _, offset, err2 = encoding.DecodeVarintAscending(val)

		it.Key().Free()
		it.Value().Free()

		if err1 != nil {
			return nil, err1
		} else if err2 != nil {
			return nil, err2
		}
		result[journal.Name(name)] = offset
	}
	return result, nil
}

// Stores |offsets| to |wb| using an identical encoding as LoadOffsetsFromDB.
func storeOffsetsToDB(wb *rocks.WriteBatch, offsets map[journal.Name]int64) {
	for name, offset := range offsets {
		wb.Put(AppendOffsetKeyEncoding(nil, name), AppendOffsetValueEncoding(nil, offset))
	}
}

// Clears offsets of |offsets|.
func clearOffsets(offsets map[journal.Name]int64) {
	for name := range offsets {
		delete(offsets, name)
	}
}

// Copies |offsets| to a new map.
func copyOffsets(offsets map[journal.Name]int64) map[journal.Name]int64 {
	copy := make(map[journal.Name]int64)
	for k, v := range offsets {
		copy[k] = v
	}
	return copy
}

// Resolves discrepancies in DB & Etcd-stored offsets. Policy is to use an Etcd
// offset only if a DB offset isn't available (eg, from a legacy V1 consumer).
func mergeOffsets(db, etcd map[journal.Name]int64) map[journal.Name]int64 {
	var result = make(map[journal.Name]int64)

	for n, o := range db {
		result[n] = o
	}
	for n, o := range etcd {
		if _, ok := result[n]; !ok {
			result[n] = o
		}
	}
	return result
}

// BlockUntilShardsAreServing uses |inspector| to observe the consumer
// allocator tree, and returns only after all shards are being served.
// This is test-support function.
func BlockUntilShardsAreServing(inspector consensus.Inspector) {
	var ch = inspector.InspectChan()

	for done := false; !done; {
		ch <- func(n *etcd.Node) {

			var items etcd.Nodes
			if dir := consensus.Child(n, consensus.ItemsPrefix); dir == nil {
				return
			} else {
				items = dir.Nodes
			}

			// Count number of items having a "primary" replica.
			var numPrimary int
			for _, item := range items {
				for _, replica := range item.Nodes {
					if replica.Value == Primary {
						numPrimary += 1
						break
					}
				}
			}

			if numPrimary == len(items) && numPrimary > 0 {
				done = true
			}
		}
	}
}
