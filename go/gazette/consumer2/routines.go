package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	dbTuple "github.com/pippio/api-server/database"
	"github.com/pippio/consensus"
	rocks "github.com/tecbot/gorocksdb"
	"golang.org/x/net/context"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
	"github.com/pippio/gazette/topic"
)

// Paired routines for loading/storing/working with hints, offsets, and
// shard <=> journal mappings. These are extracted from and unit-tested
// independently of their uses in `master` and `replica`.

const (
	hintsPrefix   = "hints"   // Etcd directory into which FSM hints are stored.
	offsetsPrefix = "offsets" // Legacy Etcd offsets path.
)

// Maps ID to padded name, eg shardName(42) => "shard-042".
func shardName(shard ShardID) string {
	return fmt.Sprintf("shard-%03d", shard)
}

// Maps a consumer |tree| and |shard| to the full path for storing FSMHints.
// Eg, hintsPath(tree{/a/consumer}, 42) => "/a/consumer/hints/shard-042".
func hintsPath(consumerPath string, shard ShardID) string {
	return consumerPath + "/" + hintsPrefix + "/" + shardName(shard)
}

// Maps |shard| to its recovery log journal.
func recoveryLog(logRoot string, shard ShardID) journal.Name {
	return journal.Name(path.Join(logRoot, shardName(shard)))
}

// Aborts processing of |shard| by |runner|. Called only in exceptional
// circumstances (eg, an unrecoverable local error).
func abort(runner *Runner, shard ShardID) {
	if err := consensus.CancelItem(runner, shardName(shard)); err != nil {
		log.WithField("err", err).Error("failed to cancel shard lock")
	}
}

// Loads JSON-encoded FSMHints from |tree| for |shard|. If hints do not exist,
// initializes new hints using the default RecoveryLogRoot root.
func loadHints(shard ShardID, runner *Runner, tree *etcd.Node) (recoverylog.FSMHints, error) {
	var hints recoverylog.FSMHints

	key := hintsPath(tree.Key, shard)
	parent, i := consensus.FindNode(tree, key)

	if i < len(parent.Nodes) && parent.Nodes[i].Key == key {
		err := json.Unmarshal([]byte(parent.Nodes[i].Value), &hints)
		return hints, err
	}

	// Persisted hints don't exist. Initialize them.
	hints.LogMark = journal.Mark{
		Journal: recoveryLog(runner.RecoveryLogRoot, shard),
		Offset:  -1,
	}
	return hints, nil
}

// Asynchronously stores JSON-encoded FSMHints to |path|.
func storeHints(keys etcd.KeysAPI, hints recoverylog.FSMHints, path string) error {
	buf, err := json.Marshal(hints)
	if err != nil {
		return err
	}

	// Actual storage of hints is best-effort.
	go func() {
		if _, err := keys.Set(context.Background(), path, string(buf), nil); err != nil {
			log.WithFields(log.Fields{"path": path, "err": err}).Warn("failed to store hints")
		}
	}()

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
func loadOffsetsFromEtcd(tree *etcd.Node) (map[journal.Name]int64, error) {
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

// Loads from |db| offsets previously serialized by storeAndClearOffsets.
func loadOffsetsFromDB(db *rocks.DB, dbRO *rocks.ReadOptions) (map[journal.Name]int64, error) {
	markPrefix := dbTuple.Tuple{"_mark"}.Pack()

	result := make(map[journal.Name]int64)

	it := db.NewIterator(dbRO)
	defer it.Close()

	for it.Seek(markPrefix); it.ValidForPrefix(markPrefix); it.Next() {
		keyBuf, valBuf := it.Key(), it.Value()

		key, err := dbTuple.UnpackTuple(keyBuf.Data())
		if err != nil {
			return nil, err
		} else if len(key) != 2 {
			return nil, fmt.Errorf("bad DB mark length %s", key)
		}

		name, ok := key[1].(string)
		if !ok {
			return nil, fmt.Errorf("bad DB mark value %s", key)
		}

		offset, err := offsetFromString(string(valBuf.Data()))
		if err != nil {
			return nil, err
		}

		keyBuf.Free()
		valBuf.Free()

		result[journal.Name(name)] = offset
	}
	return result, nil
}

// Stores |offsets| to |wb| using an identical encoding as loadOffsetsFromDB.
func storeOffsets(wb *rocks.WriteBatch, offsets map[journal.Name]int64) {
	for name, offset := range offsets {
		key := dbTuple.Tuple{"_mark", string(name)}.Pack()
		value := []byte(offsetToString(offset))

		wb.Put(key, value)
	}
}

// Clears offsets of |offsets|.
func clearOffsets(offsets map[journal.Name]int64) {
	for name := range offsets {
		delete(offsets, name)
	}
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

// Determines the number of shards implied by a consumption of |topics|,
// or returns error if |topics| have incompatible partition counts.
func numShards(topics []*topic.Description) (int, error) {
	var n int
	for _, t1 := range topics {
		for _, t2 := range topics {
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
func journalsForShard(topics []*topic.Description, shard ShardID) map[journal.Name]*topic.Description {
	var journals = make(map[journal.Name]*topic.Description)
	for _, topic := range topics {
		journals[topic.Journal(int(shard)%topic.Partitions)] = topic
	}
	return journals
}
