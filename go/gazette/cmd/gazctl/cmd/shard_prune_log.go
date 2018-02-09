package cmd

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/LiveRamp/gazette/pkg/journal"
)

var shardPruneLogCmd = &cobra.Command{
	Use:   "prune-log [etcd-hints-path]",
	Short: "Prune log removes fragments of a hinted recovery log which are no longer needed",
	Long: `
Recovery logs capture every write which has ever occurred in a Shard DB.
This includes all prior writes of client keys & values, and also RocksDB
compactions, which can significantly inflate the total volume of writes
relative to the data currently represented in a RocksDB.

Prune log examines the provided hints to identify Fragments of the log
which have no intersection with any live files of the DB, and can thus
be safely deleted.

It is recommended to run this tool against the ".lastRecovered" hints
written by consumers after recovering & becoming primary for a shard.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		if dryRun {
			log.Info("Running in dry-run mode. Pass -dry-run=false to disable")
		}

		var hints = loadHints(args[0])

		var _, segments, err = hints.LiveLogSegments()
		if err != nil {
			log.WithField("err", err).Fatal("failed to determine live log segments")
		} else if len(segments) == 0 {
			log.WithField("hints", hints).Fatal("hints include no live log segments")
		}

		// Zero the LastOffset of the final hinted Segment. This has the effect of implicitly
		// intersecting all subsequent fragments (having offsets greater than its FirstOffset).
		// We want this behavior because playback will continue to read offsets & Fragments
		// after reading past the final hinted Segment.
		segments[len(segments)-1].LastOffset = 0

		var deleteCh = make(chan journal.Fragment)
		var wg sync.WaitGroup

		if !dryRun {
			userConfirms(fmt.Sprintf("WARNING: Really prune fragments of %s? This cannot be undone.", hints.Log))

			for i := 0; i != concurrentDeletes; i++ {
				wg.Add(1)

				go func() {
					defer wg.Done()

					for f := range deleteCh {
						if err := cloudFS().Remove(f.ContentPath()); err != nil {
							log.WithFields(log.Fields{"err": err, "path": f.ContentPath()}).Warn("failed to delete fragment")
						}
					}
				}()
			}
		}

		var nTotal, nPruned, bytesTotal, bytesPruned int64

		if err = cloudFS().Walk(hints.Log.String(), journal.NewWalkFuncAdapter(func(f journal.Fragment) error {
			nTotal += 1
			bytesTotal += f.Size()

			if len(segments.Intersect(f.Begin, f.End)) == 0 {
				log.WithFields(log.Fields{
					"log":  f.Journal,
					"name": f.ContentName(),
					"size": f.Size(),
					"mod":  f.RemoteModTime,
				}).Warn("pruning fragment")

				nPruned += 1
				bytesPruned += f.Size()

				if !dryRun {
					deleteCh <- f
				}
			}
			return nil
		})); err != nil {
			log.WithFields(log.Fields{"err": err, "log": hints.Log}).Fatal("failed to walk directory")
		}

		close(deleteCh)
		wg.Wait()

		log.WithFields(log.Fields{
			"log":         hints.Log,
			"nTotal":      nTotal,
			"nPruned":     nPruned,
			"nLive":       nTotal - nPruned,
			"bytesTotal":  bytesTotal,
			"bytesPruned": bytesPruned,
			"bytesLive":   bytesTotal - bytesPruned,
		}).Info("finished pruning log")
	},
}

const concurrentDeletes = 10

var dryRun bool

func init() {
	shardCmd.AddCommand(shardPruneLogCmd)

	shardPruneLogCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", true,
		"Perform a dry-run (don't actually delete fragments)")
	shardPruneLogCmd.Flags().BoolVarP(&defaultYes, "yes", "y", false,
		"Append without asking for confirmation.")
}
