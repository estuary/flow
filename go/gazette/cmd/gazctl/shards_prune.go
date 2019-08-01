package main

import (
	"context"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/fragment"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/recoverylog"
)

type cmdShardsPrune struct {
	pruneConfig
}

func init() {
	_ = mustAddCmd(cmdShards, "prune", "Removes fragments of a hinted recovery log which are no longer needed", `
Recovery logs capture every write which has ever occurred in a Shard DB.
This includes all prior writes of client keys & values, and also RocksDB
compactions, which can significantly inflate the total volume of writes
relative to the data currently represented in a RocksDB.

Prune log examines the provided hints to identify Fragments of the log
which have no intersection with any live files of the DB, and can thus
be safely deleted.
`, &cmdShardsPrune{})
}

func (cmd *cmdShardsPrune) Execute([]string) error {
	startup()
	var ctx = context.Background()

	var m = shardsPruneMetrics{}
	for _, shard := range listShards(cmd.Selector).Shards {
		m.shardsTotal++
		var lastHints = fetchLastHints(ctx, shard.Spec.Id)
		if lastHints == nil {
			log.Infof("skipping shard %s, there are no backup hints for this shard", shard.Spec.Id)
			continue
		}

		var _, segments, err = lastHints.LiveLogSegments()
		if err != nil {
			mbp.Must(err, "unable to fetch hint segments")
		}

		// Zero the LastOffset of the final hinted Segment. This has the effect of implicitly
		// intersecting with all fragments having offsets greater than its FirstOffset.
		// We want this behavior because playback will continue to read offsets & Fragments
		// after reading past the final hinted Segment.
		segments[len(segments)-1].LastOffset = 0

		for _, f := range fetchFragments(ctx, lastHints.Log) {
			var spec = f.Spec

			m.fragmentsTotal++
			m.bytesTotal += spec.ContentLength()

			if len(segments.Intersect(spec.Begin, spec.End)) == 0 {
				log.WithFields(log.Fields{
					"log":  spec.Journal,
					"name": spec.ContentName(),
					"size": spec.ContentLength(),
					"mod":  spec.ModTime,
				}).Info("pruning fragment")

				m.fragmentsPruned++
				m.bytesPruned += spec.ContentLength()

				if !cmd.DryRun {
					err = fragment.Remove(ctx, spec)
					if err != nil {
						mbp.Must(err, "error removing fragment", "path", spec.ContentPath())
					}

				}
			}
		}
		logShardsPruneMetrics(m, shard.Spec.Id.String(), "finished pruning log for shard")
	}
	logShardsPruneMetrics(m, "", "finished pruning log for all shards")
	return nil
}

func fetchLastHints(ctx context.Context, id consumer.ShardID) *recoverylog.FSMHints {
	var req = &consumer.GetHintsRequest{
		Shard: id,
	}

	var resp, err = consumer.FetchHints(ctx, shardsCfg.Consumer.MustShardClient(ctx), req)
	mbp.Must(err, "failed to fetch hints")
	if resp.Status != consumer.Status_OK {
		log.Panic("failed to fetch hints ", resp.Status.String())
	}

	for i := len(resp.BackupHints) - 1; i >= 0; i-- {
		if resp.BackupHints[i].Hints != nil {
			return resp.BackupHints[i].Hints
		}
	}

	return nil
}

func fetchFragments(ctx context.Context, journal pb.Journal) []pb.FragmentsResponse__Fragment {
	var err error
	var req = pb.FragmentsRequest{
		Journal: journal,
	}
	var brokerClient = journalsCfg.Broker.MustRoutedJournalClient(ctx)

	resp, err := client.ListAllFragments(ctx, brokerClient, req)
	mbp.Must(err, "failed to fetch fragments")

	return resp.Fragments
}

type shardsPruneMetrics struct {
	shardsTotal     int64
	fragmentsTotal  int64
	fragmentsPruned int64
	bytesTotal      int64
	bytesPruned     int64
}

func logShardsPruneMetrics(m shardsPruneMetrics, shard, message string) {
	var fields = log.Fields{
		"shardsTotal":     m.shardsTotal,
		"fragmentsTotal":  m.fragmentsTotal,
		"fragmentsPruned": m.fragmentsPruned,
		"fragmentsKept":   m.fragmentsTotal - m.fragmentsPruned,
		"bytesTotal":      m.bytesTotal,
		"bytesPruned":     m.bytesPruned,
		"bytesKept":       m.bytesTotal - m.bytesPruned,
	}
	if shard != "" {
		fields["shard"] = shard
	}
	log.WithFields(fields).Info(message)
}
