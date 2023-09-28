package gazctlcmd

import (
	"context"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdShardsPrune struct {
	pruneConfig
}

func init() {
	CommandRegistry.AddCommand("shards", "prune", "Removes fragments of a hinted recovery log which are no longer needed", `
Recovery logs capture every write which has ever occurred in a Shard DB.
This includes all prior writes of client keys & values, and also RocksDB
compactions, which can significantly inflate the total volume of writes
relative to the data currently represented in a RocksDB.

Prune log examines the provided hints to identify Fragments of the log
which have no intersection with any live files of the DB, and can thus
be safely deleted.

CAUTION:

When pruning recovery logs which have been forked from other logs,
it's crucial that *all* shards which participate in the forked log
history are included in the prune operation. When pruning a log,
hints from all referencing shards are inspected to determine if a
fragment is overlapped, and a failure to include a shard which
references the log may cause data it depends on to be deleted.
`, &cmdShardsPrune{})
}

func (cmd *cmdShardsPrune) Execute([]string) error {
	startup(ShardsCfg.BaseConfig)

	var ctx = context.Background()
	var rsc = ShardsCfg.Consumer.MustRoutedShardClient(ctx)
	var rjc = ShardsCfg.Broker.MustRoutedJournalClient(ctx)

	var metrics = shardsPruneMetrics{}
	var logSegmentSets = make(map[pb.Journal]recoverylog.SegmentSet)
	var skipRecoveryLogs = make(map[pb.Journal]bool)

	for _, shard := range listShards(rsc, cmd.Selector).Shards {
		metrics.shardsTotal++

		var allHints, err = consumer.FetchHints(ctx, rsc, &pc.GetHintsRequest{
			Shard: shard.Spec.Id,
		})
		mbp.Must(err, "failed to fetch hints")

		var recoveryLog = shard.Spec.RecoveryLog()

		for _, curHints := range append(allHints.BackupHints, allHints.PrimaryHints) {
			var hints = curHints.Hints

			// We require that we see _all_ hints for a shards before we may make _any_ deletions.
			// This is because shards could technically include segments from any log,
			// and without comprehensive hints which are proof-positive that _no_ shard
			// references a given journal fragment, we cannot be sure it's safe to remove.
			// For this reason, we must track the journals to be skipped, so we can be sure
			// we don't prune journals that are used by a shard that hasn't persisted hints.
			if hints != nil && len(hints.LiveNodes) > 0 {
				foldHintsIntoSegments(*hints, logSegmentSets)
			} else {
				skipRecoveryLogs[recoveryLog] = true
				metrics.skippedJournals++
				var reason = "has not written all hints required for pruning"
				if hints != nil {
					reason = "hints have no live files"
				}
				log.WithFields(log.Fields{
					"shard":   shard.Spec.Id,
					"reason":  reason,
					"journal": recoveryLog,
				}).Warn("will skip pruning recovery log journal")

				break
			}
		}
	}

	for journal, segments := range logSegmentSets {
		if skipRecoveryLogs[journal] {
			log.WithField("journal", journal).Warn("skipping journal because another shard is missing hints that cover it")
			continue
		}
		log.WithField("journal", journal).Debug("checking fragments of journal")
		for _, f := range fetchFragments(ctx, rjc, journal) {
			var spec = f.Spec

			metrics.fragmentsTotal++
			metrics.bytesTotal += spec.ContentLength()

			if len(segments.Intersect(journal, spec.Begin, spec.End)) == 0 {
				log.WithFields(log.Fields{
					"log":  spec.Journal,
					"name": spec.ContentName(),
					"size": spec.ContentLength(),
					"mod":  spec.ModTime,
				}).Info("pruning fragment")

				metrics.fragmentsPruned++
				metrics.bytesPruned += spec.ContentLength()

				if !cmd.DryRun {
					mbp.Must(fragment.Remove(ctx, spec), "error removing fragment", "path", spec.ContentPath())
				}
			}
		}
		logShardsPruneMetrics(metrics, journal.String(), "finished pruning log")
	}
	logShardsPruneMetrics(metrics, "", "finished pruning logs for all shards")
	return nil
}

func fetchFragments(ctx context.Context, journalClient pb.RoutedJournalClient, journal pb.Journal) []pb.FragmentsResponse__Fragment {
	var err error
	var req = pb.FragmentsRequest{
		Journal: journal,
	}

	resp, err := client.ListAllFragments(ctx, journalClient, req)
	mbp.Must(err, "failed to fetch fragments")

	return resp.Fragments
}

func foldHintsIntoSegments(hints recoverylog.FSMHints, sets map[pb.Journal]recoverylog.SegmentSet) {
	var _, segments, err = hints.LiveLogSegments()
	if err != nil {
		mbp.Must(err, "unable to fetch hint segments")
	} else if len(segments) == 0 {
		panic("segment is empty") // We check this prior to calling in.
	}

	// Zero the LastOffset of the final hinted Segment. This has the effect of implicitly
	// intersecting with all fragments having offsets greater than its FirstOffset.
	// We want this behavior because playback will continue to read offsets & Fragments
	// after reading past the final hinted Segment.
	segments[len(segments)-1].LastOffset = 0

	for _, segment := range segments {
		var set = sets[segment.Log]

		// set.Add() will return an error if we attempt to add a segment having a
		// greater SeqNo and LastOffset != 0, to a set already having a lesser
		// SeqNo and LastOffset == 0. Or, if FirstSeqNo is equal, it will replace
		// a zero LastOffset with a non-zero one (which is not what we want
		// in this case).
		//
		// So, zero LastOffset here if |segment| isn't strictly less than
		// and non-overlapping with a pre-existing last LastOffset==0 element.
		//
		// Conceptually, we've defined a "tail" of the log where we won't delete
		// anything, and are letting the /oldest/ hints bound how early that tail
		// portion begins.
		if l := len(set); l != 0 && set[l-1].LastOffset == 0 && set[l-1].FirstSeqNo <= segment.LastSeqNo {
			segment.LastOffset = 0
		}

		mbp.Must(set.Add(segment), "failed to add segment", "log", segment.Log)
		sets[segment.Log] = set
	}
}

type shardsPruneMetrics struct {
	shardsTotal     int64
	fragmentsTotal  int64
	fragmentsPruned int64
	bytesTotal      int64
	bytesPruned     int64
	skippedJournals int64
}

func logShardsPruneMetrics(m shardsPruneMetrics, journal, message string) {
	var fields = log.Fields{
		"shardsTotal":     m.shardsTotal,
		"fragmentsTotal":  m.fragmentsTotal,
		"fragmentsPruned": m.fragmentsPruned,
		"fragmentsKept":   m.fragmentsTotal - m.fragmentsPruned,
		"bytesTotal":      m.bytesTotal,
		"bytesPruned":     m.bytesPruned,
		"bytesKept":       m.bytesTotal - m.bytesPruned,
		"skippedJournals": m.skippedJournals,
	}
	if journal != "" {
		fields["journal"] = journal
	}
	log.WithFields(fields).Info(message)
}
