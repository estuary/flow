// gazretention is used to delete fragments from cloud storage that we no longer
// wish to retain.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/cloudstore"
	"github.com/pippio/endpoints"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
	_ "github.com/pippio/graph" // Register topics
	"github.com/pippio/topics"
	"github.com/pippio/varz"
)

var (
	mode = flag.String("mode", "ask", "Options are 'ask', 'print', and 'silent'."+
		"'print' mode shows contents of fragments as they are being parsed before deletion."+
		"'ask' mode prompts for confirmation before deleting fragments. 'print' is enabled here too."+
		"'silent' runs the retention without showing parsed fragments or asking for confirmation.")
	dryRun = flag.Bool("dryRun", true,
		"If true, show fragments that are eligible for deletion without deleting them.")
	topicList topics.Flag
)

type cfsFragment struct {
	os.FileInfo
	path string
}

func appendExpiredJournalFragments(jname journal.Name, horizon time.Time,
	cfs cloudstore.FileSystem, fragments []cfsFragment) ([]cfsFragment, error) {
	// Get all fragments associated with journal older than retention time.
	if err := cfs.Walk(string(jname), func(fname string, finfo os.FileInfo, err error) error {
		var modTime = finfo.ModTime()
		if strings.Compare(*mode, "silent") != 0 {
			log.WithFields(log.Fields{
				"cfsPath":     fname,
				"fragment":    finfo.Name(),
				"sizeMb":      float64(finfo.Size()) / 1024.0 / 1024.0,
				"lastModTime": modTime,
			}).Info("Scanning fragment for retention...")
		}
		if modTime.Before(horizon) {
			fragments = append(fragments, cfsFragment{finfo, fname})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return fragments, nil
}

func appendExpiredTopicFragments(tname *topic.Description, cfs cloudstore.FileSystem,
	fragments []cfsFragment) ([]cfsFragment, error) {
	var now = time.Now()
	var horizon time.Time
	var err error
	// If retention duration is unspecified, set horizon to unix epoch.
	if tname.RetentionDuration == 0 {
		horizon = time.Unix(0, 0)
	} else {
		horizon = now.Add(-tname.RetentionDuration)
	}
	for part := 0; part < tname.Partitions; part++ {
		fragments, err = appendExpiredJournalFragments(tname.Journal(part),
			horizon, cfs, fragments)
		if err != nil {
			break
		}
	}

	return fragments, err
}

func deleteFragments(cfs cloudstore.FileSystem, toDelete []cfsFragment) error {
	// Delete the fragments.
	for _, frag := range toDelete {
		log.WithField("fragment", frag.path).Info("deleting...")
		if err := cfs.Remove(frag.path); err != nil {
			return err
		}
	}

	return nil
}

func displayAndLogFragmentsInfo(fragments []cfsFragment) {
	if len(fragments) < 1 {
		log.Info("No expired fragments found.")
		return
	}
	for _, frag := range fragments {
		// To log file.
		log.WithFields(log.Fields{
			"cfsPath":     frag.path,
			"fragment":    frag.Name(),
			"sizeMb":      float64(frag.Size()) / 1024.0 / 1024.0,
			"lastModTime": frag.ModTime(),
		}).Info("Expired fragment found.")
	}
}

func checkThenDelete(cfs cloudstore.FileSystem, toDelete []cfsFragment) error {
	var reader = bufio.NewReader(os.Stdin)
	var err error
	fmt.Println("Enter \"Y\" to remove expired fragments:")
	var cmd, _ = reader.ReadString('\n')
	cmd = strings.Replace(cmd, "\n", "", -1)
	if cmd == "Y" {
		err = deleteFragments(cfs, toDelete)
	} else {
		fmt.Println("Fragment deletion aborted.")
	}
	return err
}

func init() {
	flag.Var(&topicList, "topic", "Topic to check for expired fragments.")
}

func main() {
	defer varz.Initialize("gazretention").Cleanup()

	var cfs, err = cloudstore.NewFileSystem(nil, *endpoints.CloudFS)
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

		// Attempt to delete expired fragments for topic.
		if !*dryRun {
			if *mode == "ask" {
				err = checkThenDelete(cfs, toDelete)
			} else {
				err = deleteFragments(cfs, toDelete)
			}
		}
		if err != nil {
			log.WithField("error deleting expired fragments", err).
				Errorf("Topic retention for %s failed.", t.Name)
		}
	}
}
