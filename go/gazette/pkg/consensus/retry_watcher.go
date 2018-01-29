package consensus

import (
	"time"

	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// RetryWatcher composes Get() and Watch() of etcd.KeysAPI to provide a
// etcd.Watcher implementation with builtin retry for watch errors.
// RetryWatcher differs from KeysAPI.Watcher() in two key ways:
//  * WatcherOptions.AfterIndex is ignored. Instead, RetryWatcher() performs
//    its own Get() on the first call to Next().
//  * Watch-related errors will be silently retried via a Get(), which is both
//    passed through and also used to re-establish watch consistency.
//    Callers must be able to handle a periodic "get" response.
func RetryWatcher(keysAPI etcd.KeysAPI, key string, getOpts *etcd.GetOptions,
	watcherOpts *etcd.WatcherOptions, refreshTicker <-chan time.Time) etcd.Watcher {

	return &retryWatcher{
		keysAPI:       keysAPI,
		key:           key,
		getOpts:       getOpts,
		watchOpts:     watcherOpts,
		refreshTicker: refreshTicker,
	}
}

type retryWatcher struct {
	keysAPI   etcd.KeysAPI
	key       string
	getOpts   *etcd.GetOptions
	watchOpts *etcd.WatcherOptions

	refreshTicker <-chan time.Time

	cur etcd.Watcher
}

func (w *retryWatcher) Next(ctx context.Context) (*etcd.Response, error) {
	// Periodically force a full tree refresh.
	select {
	case <-w.refreshTicker:
		w.cur = nil
	default:
	}
	if w.cur != nil {
		r, err := w.cur.Next(ctx)

		if etcdErr, ok := err.(etcd.Error); ok {
			// If the error code indicates that further Next() attempts will fail,
			// clear the current etcd.Watcher to force a full tree refresh.
			if etcdErr.Code == etcd.ErrorCodeEventIndexCleared ||
				etcdErr.Code == etcd.ErrorCodeWatcherCleared {
				w.cur = nil
			}
		}
		return r, err
	}
	// No current Watcher. Perform a full tree refresh.
	r, err := w.keysAPI.Get(ctx, w.key, w.getOpts)

	if err == nil {
		var opts = *w.watchOpts // Clone & update.
		opts.AfterIndex = r.Index

		w.cur = w.keysAPI.Watcher(w.key, &opts)
	}
	return r, err
}
