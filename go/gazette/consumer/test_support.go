package consumer

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/LiveRamp/gazette/journal"
	"github.com/LiveRamp/gazette/recoverylog"
	"github.com/LiveRamp/gazette/topic"
)

// Test support function. Initializes all shards of |runner| to empty database
// which begin consumption from the current write-head of each topic journal.
func ResetShardsToJournalHeads(runner *Runner) error {
	for id, partition := range EnumerateShards(runner.Consumer) {
		if err := resetShard(runner, id, partition); err != nil {
			return err
		}
	}
	return nil
}

func resetShard(runner *Runner, id ShardID, partition topic.Partition) error {
	// Determine the write head of the partition Journal.
	if err := runner.Gazette.Create(partition.Journal); err != nil && err != journal.ErrExists {
		return err
	}
	var result, _ = runner.Gazette.Get(journal.ReadArgs{Journal: partition.Journal, Offset: -1})
	if result.Error != journal.ErrNotYetAvailable {
		return result.Error
	}
	var offset = result.WriteHead

	log.WithFields(log.Fields{"partition": partition.Journal, "offset": offset}).
		Info("resetting logs to write heads")

	var options = rocks.NewDefaultOptions()
	defer options.Destroy()

	// Initialize a database for the Shard at the recovery-log head which
	// captures |offset| but is otherwise empty. Then store hints to Etcd.
	var opLog = recoveryLog(runner.RecoveryLogRoot, id)

	fsm, err := recoverylog.NewFSM(recoverylog.FSMHints{Log: opLog})
	if err != nil {
		return err
	}

	localDir, err := ioutil.TempDir("", "reset-shards")
	if err != nil {
		return err
	}
	defer os.RemoveAll(localDir)

	// Open the database & store offsets,
	db, err := newDatabase(options, fsm, localDir, runner.Gazette)
	if err != nil {
		return err
	}
	storeOffsetsToDB(db.writeBatch, map[journal.Name]int64{partition.Journal: offset})

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
	return nil
}
