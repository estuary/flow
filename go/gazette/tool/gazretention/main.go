// gazretention is used to expose fragments from cloud storage that we no longer
// wish to retain.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/cloudstore"
	"github.com/pippio/gazette/envflag"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
	_ "github.com/pippio/graph" // Register topics
	"github.com/pippio/topics"
	"github.com/pippio/varz"
)

const (
	// See https://cloud.google.com/storage/pricinghttps://cloud.google.com/storage/pricing
	// for detail.
	gcpGbMonth = 0.026
	oneMb      = 1024.0 * 1024.0
	gcsPrefix  = "gs://"
)

var (
	topicList      topics.Flag
	retentionStats = make(statsMap)
	currentTopic   *topic.Description
	cloudFSUrl     = envflag.NewCloudFSURL()
)

type statsMap map[*topic.Description]map[journal.Name]*journalStats

type journalStats struct {
	name        journal.Name
	totalSize   float64
	totalFiles  int
	deleteSize  float64
	deleteFiles int
}

type cfsFragment struct {
	os.FileInfo
	path string
	ver  interface{}
}

func appendExpiredJournalFragments(jname journal.Name, horizon time.Time,
	cfs cloudstore.FileSystem, fragments []cfsFragment) ([]cfsFragment, error) {
	if _, ok := retentionStats[currentTopic][jname]; !ok {
		retentionStats[currentTopic][jname] = &journalStats{name: jname}
	}
	// Get all fragments associated with journal older than retention time.
	if err := cfs.Walk(string(jname), func(fname string, finfo os.FileInfo, err error) error {
		var modTime = finfo.ModTime()
		var sizeMb = float64(finfo.Size()) / oneMb
		log.WithFields(log.Fields{
			"cfsPath":     fname,
			"fragment":    finfo.Name(),
			"sizeMb":      float64(finfo.Size()) / oneMb,
			"lastModTime": modTime,
		}).Debug("Scanning fragment for retention...")

		var jStats = retentionStats[currentTopic][jname]
		jStats.totalFiles += 1
		jStats.totalSize += sizeMb

		if modTime.Before(horizon) {
			var ver interface{}
			if *cloudFSUrl == gcsPrefix {
				ver = finfo.(cloudstore.File).Version()
			}
			fragments = append(fragments, cfsFragment{finfo, fname, ver})
			jStats.deleteSize += sizeMb
			jStats.deleteFiles += 1
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return fragments, nil
}

func appendExpiredTopicFragments(desc *topic.Description, cfs cloudstore.FileSystem,
	fragments []cfsFragment) ([]cfsFragment, error) {
	var now = time.Now()
	var horizon time.Time
	var err error
	// If retention duration is unspecified, set horizon to unix epoch.
	if desc.RetentionDuration == 0 {
		horizon = time.Unix(0, 0)
	} else {
		horizon = now.Add(-desc.RetentionDuration)
	}
	if _, ok := retentionStats[desc]; !ok {
		retentionStats[desc] = make(map[journal.Name]*journalStats)
	}
	currentTopic = desc
	for _, part := range desc.Partitions() {
		fragments, err = appendExpiredJournalFragments(part, horizon, cfs, fragments)
		if err != nil {
			break
		}
	}

	return fragments, err
}

func displayAndLogFragmentsInfo(fragments []cfsFragment) {
	if len(fragments) < 1 {
		log.Info("No expired fragments found.")
		return
	}
	for _, frag := range fragments {
		log.WithFields(log.Fields{
			"cfsPath":     frag.path,
			"fragment":    frag.Name(),
			"sizeMb":      float64(frag.Size()) / oneMb,
			"lastModTime": frag.ModTime(),
		}).Debug("Expired fragment found.")
		if *cloudFSUrl == gcsPrefix {
			fmt.Printf(gcsPrefix+frag.path+"#%v\n", frag.ver)
		} else {
			fmt.Println(frag.path)
		}
	}
}

func calculateAndLogSummaryStats(retentionStats statsMap) {
	// Display stats about fragments found for deletion.
	var totalMb float64
	var deleteMb float64
	var totalFiles int
	var deleteFiles int
	var deletePercentage float64

	var topicTotalMb float64
	var topicDeleteMb float64
	var topicTotalFiles int
	var topicDeleteFiles int
	for tname, jmap := range retentionStats {
		topicTotalMb = 0
		topicDeleteMb = 0
		topicTotalFiles = 0
		topicDeleteFiles = 0
		for _, jstats := range jmap {
			topicTotalMb += jstats.totalSize
			topicDeleteMb += jstats.deleteSize
			topicTotalFiles += jstats.totalFiles
			topicDeleteFiles += jstats.deleteFiles
		}
		var topicTotalGb = topicTotalMb / 1024.0
		var topicDeleteGb = topicDeleteMb / 1024.0
		deletePercentage = 0
		if topicTotalGb > 0 {
			deletePercentage = 100 * topicDeleteGb / topicTotalGb
		}
		var topicFields = log.Fields{
			"name":             tname.Name,
			"totalGb":          topicTotalGb,
			"deleteGb":         topicDeleteGb,
			"totalCost":        topicTotalGb * gcpGbMonth,
			"deleteCost":       topicDeleteGb * gcpGbMonth,
			"totalFiles":       topicTotalFiles,
			"deleteFiles":      topicDeleteFiles,
			"deletePercentage": deletePercentage,
		}
		log.WithFields(topicFields).Info("Topic stats")
		for jname, jstats := range jmap {
			var journalTotalGb = jstats.totalSize / 1024.0
			var journalDeleteGb = jstats.deleteSize / 1024.0
			deletePercentage = 0
			if journalTotalGb > 0 {
				deletePercentage = 100 * journalDeleteGb / journalTotalGb
			}
			var journalFields = log.Fields{
				"name":             jname,
				"totalGb":          journalTotalGb,
				"deleteGb":         journalDeleteGb,
				"totalCost":        journalTotalGb * gcpGbMonth,
				"deleteCost":       journalDeleteGb * gcpGbMonth,
				"totalFiles":       jstats.totalFiles,
				"deleteFiles":      jstats.deleteFiles,
				"deletePercentage": deletePercentage,
			}
			log.WithFields(journalFields).Info("Journal stats")
		}
		totalMb += topicTotalMb
		deleteMb += topicDeleteMb
		totalFiles += topicTotalFiles
		deleteFiles += topicDeleteFiles
	}

	deletePercentage = 0
	if totalMb > 0 {
		deletePercentage = 100 * deleteMb / totalMb
	}
	var totalFields = log.Fields{
		"name":             "total",
		"totalGb":          totalMb / 1024.0,
		"deleteGb":         deleteMb / 1024.0,
		"totalCost":        gcpGbMonth * totalMb / 1024.0,
		"deleteCost":       gcpGbMonth * deleteMb / 1024.0,
		"totalFiles":       totalFiles,
		"deleteFiles":      deleteFiles,
		"deletePercentage": deletePercentage,
	}
	log.WithFields(totalFields).Info("Total stats")
}

func init() {
	flag.Var(&topicList, "topic", "Topic to check for expired fragments.")
}

func main() {
	envflag.Parse()
	defer varz.InitializeStandalone("gazretention").Cleanup()

	log.SetFormatter(&log.JSONFormatter{})

	var cfs, err = cloudstore.NewFileSystem(nil, *cloudFSUrl)
	if err != nil {
		log.WithField("err", err).Fatal("cannot initialize cloudstore")
	}

	// Check the "-topic" flag presence, if not specified, use all topics.
	var tList []*topic.Description
	if len(topicList.Topics) == 0 {
		for _, t := range topics.ByName {
			tList = append(tList, t)
		}
	} else {
		tList = topicList.Topics
	}

	// For each topic, gather expired fragments, log, and delete.
	for _, t := range tList {
		var toDelete []cfsFragment
		log.WithField("topic", t).Info("Evaluating topic for retention.")

		// Attempt to gather expired fragments for topic.
		toDelete, err = appendExpiredTopicFragments(t, cfs, toDelete)
		if err != nil {
			log.WithField("error gathering fragments", err).
				Errorf("Topic retention for %s failed.", t.Name)
			return
		}
		displayAndLogFragmentsInfo(toDelete)
	}

	calculateAndLogSummaryStats(retentionStats)
}
