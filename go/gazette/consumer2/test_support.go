package consumer

import (
	"io/ioutil"
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
)

// Test support function. Initializes all shards of |runner| to empty database
// which begin consumption from the current write-head of each topic journal.
func ResetShardsToJournalHeads(runner *Runner) error {
	// Determine the write-heads of all journals of all topics.
	var offsets = make(map[journal.Name]int64)
	for _, topic := range runner.Consumer.Topics() {
		for part := 0; part != topic.Partitions; part++ {
			offsets[topic.Journal(part)] = 0
		}
	}

	for name := range offsets {
		result, _ := runner.Getter.Get(journal.ReadArgs{Journal: name, Offset: -1})
		if result.Error != journal.ErrNotYetAvailable {
			return result.Error
		}
		offsets[name] = result.WriteHead
	}
	log.WithField("offsets", offsets).Info("resetting logs to write heads")

	shards, err := numShards(runner.Consumer.Topics())
	if err != nil {
		return err
	}

	// For each shard, initialize a database at the recovery-log head which
	// captures |offsets| but is otherwise empty. Then store hints to Etcd.
	for shard := 0; shard != shards; shard++ {
		var opLog = recoveryLog(runner.RecoveryLogRoot, ShardID(shard))

		// Determine recovery-log head.
		result, _ := runner.Getter.Get(journal.ReadArgs{Journal: opLog, Offset: -1})
		if result.Error != journal.ErrNotYetAvailable {
			return result.Error
		}

		var fsm = recoverylog.NewFSM(recoverylog.FSMHints{
			LogMark: journal.Mark{Journal: opLog, Offset: result.WriteHead}})

		localDir, err := ioutil.TempDir("", "reset-shards")
		if err != nil {
			return err
		}
		defer os.RemoveAll(localDir)

		// Open the database & store offsets,
		db, err := newDatabase(fsm, localDir, runner.Writer)
		if err != nil {
			return err
		}
		storeOffsets(db.writeBatch, offsets)

		// Commit, and store resulting hints to Etcd.
		barrier, err := db.commit()
		if err != nil {
			return err
		}

		hints := db.recorder.BuildHints()
		hintsPath := hintsPath(runner.ConsumerRoot, ShardID(shard))

		log.WithFields(log.Fields{"shard": shard, "hints": hints, "path": hintsPath}).
			Info("storing hints")

		if err = storeHints(runner.KeysAPI(), hints, hintsPath); err != nil {
			return err
		}

		// Wait for Gazette to commit.
		<-barrier
	}
	return nil
}
