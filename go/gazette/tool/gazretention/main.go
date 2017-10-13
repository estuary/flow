// gazretention is used to expose fragments from cloud storage that we no longer
// wish to retain.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/pippio/gazette/cloudstore"
	"github.com/pippio/gazette/envflagfactory"
	"github.com/pippio/gazette/mainboilerplate"
	"github.com/pippio/gazette/metrics"
)

const (
	oneMb = 1024 * 1024
)

var (
	prefix = flag.String("prefix", "", "The directory prefix to search.")
	dur    = flag.String("dur", "0s",
		"Files found that are older than |dur| from now will be returned.")
	config = flag.String("config", "",
		"Filepath to JSON config file of an array of prefix and durations.")
	nprocs = flag.Int("nprocs", 8,
		"How many deletes to run in parallel")
	cloudFSUrl = envflagfactory.NewCloudFSURL()
)

type cfsFragment struct {
	os.FileInfo
	path   string
	prefix string
}

// findExpiredFragments searches the provided cloudstore filesystem |cfs| under
// the directory specified by |prefix| and returns any files found modified
// after now - |duration|.
func appendExpiredFragments(prefix string, duration time.Duration,
	frags []*cfsFragment, cfs cloudstore.FileSystem) ([]*cfsFragment, error) {
	var horizon time.Time

	// Default of a 0 duration keeps any files written after unix epoch.
	if duration == 0 {
		horizon = time.Unix(0, 0)
	} else {
		horizon = time.Now().Add(-duration)
	}

	// Get all fragments associated with journal older than retention time.
	if err := cfs.Walk(prefix, func(fname string, finfo os.FileInfo, err error) error {
		var modTime = finfo.ModTime()

		var sizeMb = float64(finfo.Size()) / oneMb

		if modTime.Before(horizon) {
			log.WithFields(log.Fields{
				"cfsPath":     fname,
				"fragment":    finfo.Name(),
				"sizeMb":      sizeMb,
				"lastModTime": modTime,
			}).Debug("Expired fragment found...")
			frags = append(frags, &cfsFragment{finfo, fname, prefix})
		} else {
			// Stats collection while we're parsing journal fragments anyway.
			// Only increment on fragments we plan on keeping.
			metrics.GazretentionRetainedFragmentsTotal.WithLabelValues(prefix).Inc()
			metrics.GazretentionRetainedBytesTotal.WithLabelValues(prefix).Add(float64(finfo.Size()))
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return frags, nil
}

// deleteExpiredFrags deletes and emits stats on expired fragments |expFrags|
// found on the filesystem |cfs|.
func deleteExpiredFrags(expFrags []*cfsFragment, cfs cloudstore.FileSystem) error {
	// Make a channel that accepts fragments.
	var ch = make(chan *cfsFragment, 256)
	var numErrs int64 = 0

	// Make a WaitGroup that tracks the completion of the goroutines.
	var wg sync.WaitGroup
	wg.Add(*nprocs)

	// Create |*nprocs| goroutines that read from the channel and delete files
	// in a loop, incrementing a stats every time.
	for i := 0; i < *nprocs; i++ {
		go func(f <-chan *cfsFragment) {
			for frag := range f {
				if err := cfs.Remove(frag.path); err != nil {
					atomic.AddInt64(&numErrs, 1)
					log.WithField("err", err).Error("error deleting file.")
				} else {
					metrics.GazretentionDeletedFragmentsTotal.
						WithLabelValues(frag.prefix).Inc()
					metrics.GazretentionDeletedBytesTotal.
						WithLabelValues(frag.prefix).Add(float64(frag.Size()))
					log.WithField("path", frag.path).Debug("deleted file.")
				}
			}
			wg.Done()
		}(ch)
	}

	// Fill the channel with the content of |expFrags|.
	for _, frag := range expFrags {
		ch <- frag
	}

	// Tell the goroutines to die.
	close(ch)

	// Wait for the goroutines to finish.
	wg.Wait()

	if numErrs > 0 {
		return fmt.Errorf("could not delete %d files.", numErrs)
	} else {
		return nil
	}
}

// readAndParseConf reads a JSON-formatted config file |fp| which contains a map
// of prefix and retention durations.
func readAndParseConf(fp string) (map[string]string, error) {
	var err error
	var raw []byte
	var confMap map[string]string

	raw, err = ioutil.ReadFile(fp)
	if err != nil {
		return confMap, err
	}

	err = json.Unmarshal(raw, &confMap)
	if err != nil {
		return confMap, err
	}

	return confMap, nil
}

func main() {
	defer mainboilerplate.LogPanic()

	mainboilerplate.Initialize()

	prometheus.MustRegister(metrics.GazretentionCollectors()...)

	var err error
	var cfs cloudstore.FileSystem
	var tdur time.Duration
	var confMap map[string]string
	var expFrags []*cfsFragment

	// Get either prefix or filepath to parse.
	if *prefix != "" {
		confMap[*prefix] = *dur
	} else if *config != "" {
		confMap, err = readAndParseConf(*config)
		if err != nil {
			log.WithField("err", err).Fatal("Failed to parse conf file")
		}
	} else {
		log.Fatal("-prefix or -config must be specified.")
	}

	cfs, err = cloudstore.NewFileSystem(nil, *cloudFSUrl)
	if err != nil {
		log.WithField("err", err).Fatal("cannot initialize cloudstore.")
	}

	for pref, duration := range confMap {
		log.WithField("prefix", pref).
			Info("Gathering expired journal fragments...")
		tdur, err = time.ParseDuration(duration)
		if err != nil {
			log.WithField("err", err).Error("invalid retention duration.")
			continue
		}
		expFrags, err = appendExpiredFragments(pref, tdur, expFrags, cfs)
		if err != nil {
			log.WithField("err", err).Error("cannot parse filesystem.")
		}
	}

	if len(expFrags) > 0 {
		log.Info("Deleting expired fragments...")
		err = deleteExpiredFrags(expFrags, cfs)
		if err != nil {
			log.WithField("err", err).Fatal("Unable to delete expired fragments.")
		}
	} else {
		log.Info("No expired fragments found!")
	}
	log.Infof("Done! Uncovered and performed delete on %d fragments.",
		len(expFrags))
}
