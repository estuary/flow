package consumer

import (
	"encoding/json"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
)

// Test support function. Initializes all shards of |runner| to empty database
// which begin consumption from the current write-head of each topic journal.
func ResetShardsToJournalHeads(runner *Runner) error {
	for _, group := range runner.Consumer.Groups() {
		if err := resetGroup(runner, group); err != nil {
			return err
		}
	}
	return nil
}

func resetGroup(runner *Runner, group TopicGroup) error {
	// Determine the write-heads of all journals of all topics.
	var offsets = make(map[journal.Name]int64)
	for _, topic := range group.Topics {
		for part := 0; part != topic.Partitions; part++ {
			offsets[topic.Journal(part)] = 0
		}
	}

	for name := range offsets {
		if err := runner.Gazette.Create(name); err != nil && err != journal.ErrExists {
			return err
		}
		result, _ := runner.Gazette.Get(journal.ReadArgs{Journal: name, Offset: -1})
		if result.Error != journal.ErrNotYetAvailable {
			return result.Error
		}
		offsets[name] = result.WriteHead
	}
	log.WithField("offsets", offsets).Info("resetting logs to write heads")

	shards, err := group.NumShards()
	if err != nil {
		return err
	}

	// For each shard, initialize a database at the recovery-log head which
	// captures |offsets| but is otherwise empty. Then store hints to Etcd.
	for shard := 0; shard != shards; shard++ {
		var id = ShardID{group.Name, shard}
		var opLog = recoveryLog(runner.RecoveryLogRoot, id)

		var fsm = recoverylog.NewFSM(recoverylog.FSMHints{
			LogMark: journal.Mark{Journal: opLog}})

		localDir, err := ioutil.TempDir("", "reset-shards")
		if err != nil {
			return err
		}
		defer os.RemoveAll(localDir)

		// Open the database & store offsets,
		db, err := newDatabase(fsm, localDir, runner.Gazette)
		if err != nil {
			return err
		}
		storeOffsetsToDB(db.writeBatch, offsets)

		// Commit, and store resulting hints to Etcd.
		barrier, err := db.commit()
		if err != nil {
			return err
		}

		var hints string
		if b, err := json.Marshal(db.recorder.BuildHints()); err != nil {
			return err
		} else {
			hints = string(b)
		}

		var path = hintsPath(runner.ConsumerRoot, id)
		log.WithFields(log.Fields{"shard": id, "hints": hints, "path": path}).
			Info("storing hints")

		if _, err := runner.KeysAPI().Set(context.Background(), path, hints, nil); err != nil {
			return err
		}

		// Wait for Gazette to commit.
		<-barrier.Ready
	}
	return nil
}
