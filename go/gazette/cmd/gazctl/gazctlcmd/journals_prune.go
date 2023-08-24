package gazctlcmd

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdJournalsPrune struct {
	pruneConfig
}

func init() {
	CommandRegistry.AddCommand("journals", "prune", "Deletes fragments older than the configured retention", `
Deletes fragments across all configured fragment stores of matching journals that are older than the configured retention.

There is a caveat when pruning journals. For a given journal, there could be multiple fragments covering the same offset. These fragments contain identical data at a given offset, but the brokers are tracking only the largest fragment, i.e. the fragment that covers the largest span of offsets. As a result, the prune command will delete only this tracked fragment, leaving the smaller fragments untouched. As a workaround, operators can wait for the fragment listing to refresh and prune the journals again.

Use --selector to supply a LabelSelector to select journals to prune.
See "journals list --help" for details and examples.
`, &cmdJournalsPrune{})
}

func (cmd *cmdJournalsPrune) Execute([]string) error {
	startup(JournalsCfg.BaseConfig)
	var ctx = context.Background()
	var rjc = JournalsCfg.Broker.MustRoutedJournalClient(ctx)

	var resp = listJournals(rjc, cmd.Selector)
	if len(resp.Journals) == 0 {
		log.WithField("selector", cmd.Selector).Panic("no journals match selector")
	}

	var m = journalsPruneMetrics{journalsTotal: len(resp.Journals)}
	var now = time.Now()
	for _, j := range resp.Journals {
		for _, f := range fetchAgedFragments(j.Spec, now, &m) {
			log.WithFields(log.Fields{
				"journal": f.Journal,
				"name":    f.ContentName(),
				"size":    f.ContentLength(),
				"mod":     f.ModTime,
			}).Info("pruning fragment")

			if !cmd.DryRun {
				err := fragment.Remove(context.Background(), f)
				mbp.Must(err, "error removing fragment", "path", f.ContentPath())
			}
			m.fragmentsPruned++
			m.bytesPruned += int(f.End - f.Begin)
		}
		m.journalsPruned++
		logJournalsPruneMetrics(m, j.Spec.Name, "pruned journal")
	}
	logJournalsPruneMetrics(m, "", "finished pruning all journals")
	return nil
}

type journalsPruneMetrics struct {
	journalsTotal  int
	journalsPruned int

	fragmentsTotal  int
	fragmentsPruned int

	// For bytesTotal and bytesPruned, the bytes refer to the size of the
	// content written into the journals. This is likely different from the
	// sum of the fragment file sizes in the backing store due to framing and
	// compression.
	bytesTotal, bytesPruned int
}

func logJournalsPruneMetrics(metrics journalsPruneMetrics, journal pb.Journal, message string) {
	var f = log.Fields{
		"journalsTotal":  metrics.journalsTotal,
		"journalsPruned": metrics.journalsPruned,

		"fragmentsTotal":  metrics.fragmentsTotal,
		"fragmentsPruned": metrics.fragmentsPruned,
		"fragmentsKept":   metrics.fragmentsTotal - metrics.fragmentsPruned,

		"bytesTotal":  metrics.bytesTotal,
		"bytesPruned": metrics.bytesPruned,
		"bytesKept":   metrics.bytesTotal - metrics.bytesPruned,
	}

	if journal != "" {
		f["journal"] = journal
	}

	log.WithFields(f).Info(message)
}

// fetchAgedFragments returns fragments of the journal that are older than the
// configured retention.
func fetchAgedFragments(spec pb.JournalSpec, now time.Time, metrics *journalsPruneMetrics) []pb.Fragment {
	var ctx = context.Background()
	var jc = JournalsCfg.Broker.MustRoutedJournalClient(ctx)
	resp, err := client.ListAllFragments(ctx, jc, pb.FragmentsRequest{Journal: spec.Name})
	mbp.Must(err, "failed to fetch fragments")

	var retention = spec.Fragment.Retention

	var aged = make([]pb.Fragment, 0)
	for _, f := range resp.Fragments {
		var spec = f.Spec
		metrics.fragmentsTotal++
		metrics.bytesTotal += int(spec.End - spec.Begin)
		if spec.BackingStore == "" {
			continue
		}
		var age = now.Sub(time.Unix(spec.ModTime, 0))
		if age >= retention {
			aged = append(aged, spec)
		}
	}

	log.WithFields(log.Fields{
		"journal": spec.Name,
		"total":   len(resp.Fragments),
		"aged":    len(aged),
	}).Info("fetched aged fragments")

	return aged
}
