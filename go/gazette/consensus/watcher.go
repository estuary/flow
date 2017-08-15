package consensus

import (
	"context"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
)

// BlockUntilModified blocks until the watcher is modified beyond
// the passed index.
func BlockUntilModified(watcher etcd.Watcher, index uint64) {
	for {
		if resp, err := watcher.Next(context.Background()); err != nil {
			log.WithField("err", err).Warn("polling for Etcd changes")
			continue
		} else if resp.Node.ModifiedIndex > index {
			log.WithFields(log.Fields{
				"initialIndex":  index,
				"modifiedIndex": resp.Node.ModifiedIndex,
			}).Info("detected Etcd change")
			break
		}
	}
}
