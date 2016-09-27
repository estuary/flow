package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/cockroachdb/cockroach/util/encoding"
	etcd "github.com/coreos/etcd/client"
	"github.com/pippio/consensus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
	"github.com/pippio/gazette/topic"
)

// Paired routines for loading/storing/working with hints, offsets, and
// shard <=> journal mappings. These are extracted from and unit-tested
// independently of their uses in `master` and `replica`.

const (
	hintsPrefix     = "hints"   // Etcd directory into which FSM hints are stored.
	offsetsPrefix   = "offsets" // Legacy Etcd offsets path.
	validGroupChars = "abcdefghijklmnopqrstuvwxyz0123456789-"
)

// Maps ID to padded name, eg shardName(42) => "shard-042".
func (id ShardID) String() string {
	return fmt.Sprintf("shard-%s-%03d", id.Group, id.Index)
}

// Ensures that a topic group has a name and consumes at least one topic.
// Note: Topic compatibility is not validated here. It is done in NumShards.
func (g TopicGroup) Validate() error {
	if g.Name == "" {
		return errors.New("a TopicGroup must have a name")
	} else if strings.Trim(g.Name, validGroupChars) != "" {
		return fmt.Errorf("a TopicGroup name must consist only of [a-z0-9-]+: %s", g.Name)
	} else if len(g.Topics) == 0 {
		return errors.New("a TopicGroup must consume at least one topic")
	}
	return nil
}

// Determines the number of shards implied by a consumption of |topics|,
// or returns error if |topics| have incompatible partition counts.
func (g TopicGroup) NumShards() (int, error) {
	var n int
	for _, t1 := range g.Topics {
		for _, t2 := range g.Topics {
			if t1.Partitions%t2.Partitions != 0 && t2.Partitions%t1.Partitions != 0 {
				return 0, errors.New("topic partitions must be multiples of each other")
			}
		}
		if t1.Partitions > n {
			n = t1.Partitions
		}
	}
	return n, nil
}

// Returns the journals |shard| should consume across |topics|.
func (g TopicGroup) JournalsForShard(shardIndex int) map[journal.Name]*topic.Description {
	var journals = make(map[journal.Name]*topic.Description)
	for _, topic := range g.Topics {
		journals[topic.Journal(shardIndex%topic.Partitions)] = topic
	}
	return journals
}

// Ensures that the topic groups are sorted by distinct group name for
// |consumer.Allocator| compliance.
func (gs TopicGroups) Validate() error {
	var lastName string
	if len(gs) == 0 {
		return errors.New("must specify at least one TopicGroup")
	}
	for _, group := range gs {
		if err := group.Validate(); err != nil {
			return err
		} else if lastName == "" || lastName < group.Name {
			lastName = group.Name
		} else {
			return fmt.Errorf("consumer groups must be sorted and names must not repeat: %s", group.Name)
		}
	}
	return nil
}

// Flattens |topics| into multiple groups, each having a single Description.
func TopicDescriptionsToSimpleGroups(topics []*topic.Description) TopicGroups {
	var groups = make(TopicGroups, len(topics))
	for i, t := range topics {
		groups[i].Name = path.Base(t.Name)
		groups[i].Topics = []*topic.Description{t}
	}
	return groups
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
		log.WithField("err", err).Error("failed to cancel shard lock")
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

// Stores |hints| in Etcd.
func storeHintsToEtcd(hintsPath string, hints string, keysAPI etcd.KeysAPI) {
	_, err := keysAPI.Set(context.Background(), hintsPath, hints, nil)
	// Etcd Set is best-effort.
	if err != nil {
		log.WithFields(log.Fields{"path": hintsPath, "err": err}).
			Warn("failed to store hints")
	}
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

// Stores legacy |offsets| in Etcd.
func StoreOffsetsToEtcd(rootPath string, offsets map[journal.Name]int64, keysAPI etcd.KeysAPI) {
	for name, offset := range offsets {
		offsetPath := OffsetPath(rootPath, name)
		_, err := keysAPI.Set(context.Background(), offsetPath,
			strconv.FormatInt(offset, 16), nil)
		// Etcd Set is best-effort.
		if err != nil {
			log.WithFields(log.Fields{"path": offsetPath, "err": err}).
				Warn("failed to store offset")
		}
	}
}

// Loads from |db| offsets previously serialized by storeAndClearOffsets.
func LoadOffsetsFromDB(db *rocks.DB, dbRO *rocks.ReadOptions) (map[journal.Name]int64, error) {
	markPrefix := encoding.EncodeNullAscending(nil)
	markPrefix = encoding.EncodeStringAscending(markPrefix, "mark")

	result := make(map[journal.Name]int64)

	it := db.NewIterator(dbRO)
	defer it.Close()

	for it.Seek(markPrefix); it.ValidForPrefix(markPrefix); it.Next() {
		key, val := it.Key().Data(), it.Value().Data()

		_, name, err1 := encoding.DecodeStringAscending(key[len(markPrefix):], nil)
		_, offset, err2 := encoding.DecodeVarintAscending(val)

		it.Key().Free()
		it.Value().Free()

		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}
		result[journal.Name(name)] = offset
	}
	return result, nil
}

// Stores |offsets| to |wb| using an identical encoding as LoadOffsetsFromDB.
func storeOffsetsToDB(wb *rocks.WriteBatch, offsets map[journal.Name]int64) {
	for name, offset := range offsets {
		key := encoding.EncodeNullAscending(nil)
		key = encoding.EncodeStringAscending(key, "mark")
		key = encoding.EncodeStringAscending(key, string(name))

		value := encoding.EncodeVarintAscending(nil, offset)

		wb.Put(key, value)
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
