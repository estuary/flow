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
	var logSegmentSets = make(map[pb.Journal][]recoverylog.Segment)
	var skipRecoveryLogs = make(map[pb.Journal]bool)
	// Retain the raw hints responses, so that we can log them if they're used to prune fragments
	var rawHintsResponses = make(map[pb.Journal][]pc.GetHintsResponse)

	for _, shard := range listShards(rsc, cmd.Selector).Shards {
		metrics.shardsTotal++

		var recoveryLog = shard.Spec.RecoveryLog()

		// Check if the recovery log's fragment stores are healthy before fetching hints.
		// Skip this shard if any store is unhealthy to avoid hanging.
		if !checkRecoveryLogStoresHealth(ctx, rjc, recoveryLog) {
			skipRecoveryLogs[recoveryLog] = true
			continue
		}

		var allHints, err = consumer.FetchHints(ctx, rsc, &pc.GetHintsRequest{
			Shard: shard.Spec.Id,
		})
		mbp.Must(err, "failed to fetch hints")

		rawHintsResponses[recoveryLog] = append(rawHintsResponses[recoveryLog], *allHints)

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
				}).Debug("will skip pruning recovery log journal")

				break
			}
		}
	}

	for journal, segments := range logSegmentSets {
		if skipRecoveryLogs[journal] {
			log.WithField("journal", journal).Warn("skipping journal because a shard is missing hints that cover it or has unlhealthy store")
			continue
		}
		log.WithField("journal", journal).Debug("checking fragments of journal")
		var prunedFragments []pb.Fragment
		for _, f := range fetchFragments(ctx, rjc, journal) {
			var spec = f.Spec

			metrics.fragmentsTotal++
			metrics.bytesTotal += spec.ContentLength()

			if !overlapsAnySegment(segments, spec) {
				if spec.ModTime == 0 {
					// This shouldn't ever happen, as long as the label selector covers all shards that are using
					// each journal. But we don't validate that up front, so failing fast is the next best thing.
					log.WithFields(log.Fields{
						"journal":        spec.Journal,
						"name":           spec.ContentName(),
						"begin":          spec.Begin,
						"end":            spec.End,
						"hintedSegments": segments,
					}).Fatal("unpersisted fragment does not overlap any hinted segments (the label selector argument does not include all shards using this log)")
				}
				log.WithFields(log.Fields{
					"log":   spec.Journal,
					"name":  spec.ContentName(),
					"size":  spec.ContentLength(),
					"mod":   spec.ModTime,
					"begin": spec.Begin,
					"end":   spec.End,
				}).Debug("pruning fragment")

				var removed = true
				if !cmd.DryRun {
					if err := fragment.Remove(ctx, spec); err != nil {
						removed = false
						metrics.failedToRemove++
						log.WithFields(log.Fields{
							"fragment": spec,
							"error":    err,
						}).Warn("failed to remove fragment (skipping)")
					}
				}
				if removed {
					metrics.fragmentsPruned++
					metrics.bytesPruned += spec.ContentLength()
					prunedFragments = append(prunedFragments, spec)
				}
			}
		}
		if len(prunedFragments) > 0 {
			log.WithFields(log.Fields{
				"journal":         journal,
				"allHints":        rawHintsResponses[journal],
				"liveSegments":    segments,
				"prunedFragments": prunedFragments,
			}).Info("pruned fragments")
		}
		logShardsPruneMetrics(metrics, journal.String(), "finished pruning log")
	}
	logShardsPruneMetrics(metrics, "", "finished pruning logs for all shards")

	if metrics.failedToRemove > 0 {
		log.WithField("failures", metrics.failedToRemove).Fatal("failed to remove fragments")
	}
	return nil
}

// checkRecoveryLogStoresHealth checks if all fragment stores for the given recovery log journal are healthy.
// Returns true if all stores are healthy, false otherwise.
func checkRecoveryLogStoresHealth(ctx context.Context, jc pb.JournalClient, recoveryLog pb.Journal) bool {
	var journalSpec, err = client.GetJournal(ctx, jc, recoveryLog)
	if err != nil {
		log.WithFields(log.Fields{
			"journal": recoveryLog,
			"error":   err,
		}).Warn("failed to fetch journal spec for recovery log")
		return false
	}

	if len(journalSpec.Fragment.Stores) == 0 {
		// No stores configured, consider healthy
		return true
	}

	for _, store := range journalSpec.Fragment.Stores {
		var resp, err = client.FragmentStoreHealth(ctx, jc, store)
		if err != nil {
			log.WithFields(log.Fields{
				"journal": recoveryLog,
				"store":   store,
				"error":   err,
			}).Warn("failed to check fragment store health")
			return false
		}

		if resp.Status != pb.Status_OK {
			log.WithFields(log.Fields{
				"journal":    recoveryLog,
				"store":      store,
				"status":     resp.Status,
				"storeError": resp.StoreHealthError,
			}).Warn("fragment store is unhealthy")
			return false
		}
	}

	return true
}

func overlapsAnySegment(segments []recoverylog.Segment, fragment pb.Fragment) bool {
	for _, seg := range segments {
		if (seg.FirstOffset < fragment.End || fragment.End == 0) &&
			(seg.LastOffset > fragment.Begin || seg.LastOffset == 0) {
			return true
		}
	}
	if len(segments) == 0 {
		log.WithFields(log.Fields{
			"log": fragment.Journal,
		}).Warn("no live segments for log")
		return true
	}
	return false
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

func foldHintsIntoSegments(hints recoverylog.FSMHints, sets map[pb.Journal][]recoverylog.Segment) {
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
		sets[segment.Log] = append(sets[segment.Log], segment)
	}
}

type shardsPruneMetrics struct {
	shardsTotal     int64
	fragmentsTotal  int64
	fragmentsPruned int64
	bytesTotal      int64
	bytesPruned     int64
	skippedJournals int64
	failedToRemove  int64
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
		"failedToRemove":  m.failedToRemove,
	}
	if journal != "" {
		fields["journal"] = journal
	}
	log.WithFields(fields).Info(message)
}
