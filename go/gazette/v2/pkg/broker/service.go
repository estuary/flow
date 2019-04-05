package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	"github.com/LiveRamp/gazette/v2/pkg/metrics"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/trace"
)

// Service is the top-level runtime concern of a Gazette Broker process. It
// drives local journal handling in response to allocator.State, powers
// journal resolution, and is also an implementation of protocol.JournalServer.
type Service struct {
	jc       pb.JournalClient
	etcd     clientv3.KV
	resolver *resolver
}

// NewService constructs a new broker Service, driven by allocator.State.
func NewService(state *allocator.State, jc pb.JournalClient, etcd clientv3.KV) *Service {
	var svc = &Service{jc: jc, etcd: etcd}

	svc.resolver = newResolver(state, func(journal pb.Journal) *replica {
		var rep = newReplica(journal)
		go svc.maintenanceLoop(rep)
		return rep
	})
	return svc
}

// Route an item using the Service resolver. Route implements the
// protocol.DispatchRouter interface, and enables usages of
// protocol.WithDispatchItemRoute (eg, `client` & `http_gateway` packages) to
// resolve items via the Service resolver.
func (svc *Service) Route(ctx context.Context, item string) pb.Route {
	var res, err = svc.resolver.resolve(resolveArgs{
		ctx:      ctx,
		journal:  pb.Journal(item),
		mayProxy: true,
	})
	if err != nil {
		panic(err) // Cannot err because we use neither minEtcdRevision nor proxyHeader.
	}
	// If Status != OK, Route will be zero-valued, which directs dispatcher
	// to use the default service address (localhost), which will then re-run
	// resolution and generate a proper error message for the client.
	return res.Route
}

// UpdateRoute is a no-op implementation of protocol.DispatchRouter.
func (svc *Service) UpdateRoute(string, *pb.Route) {} // No-op.
// IsNoopRouter returns false.
func (svc *Service) IsNoopRouter() bool { return false }

// maintenanceLoop performs periodic tasks over a replica:
//  - Refreshing its remote fragment listings from configured stores.
//  - Pinging the journal pipeline to ensure its live-ness, and the
//    consistency of allocator assignment values stored in Etcd.
func (svc *Service) maintenanceLoop(r *replica) {
	// Start a timer which triggers refreshes of remote journal fragments. The
	// duration between each refresh can change based on current configurations,
	// so each refresh iteration resets the timer with the next interval.
	var refreshTimer = time.NewTimer(0)
	defer refreshTimer.Stop()
	// We ping the journal pipeline periodically, and also on-demand when signalled.
	var pingTicker = time.NewTicker(healthCheckInterval)
	defer pingTicker.Stop()
	// Minimum Etcd revision we must read through on next resolution.
	var minRevision int64

	for {
		var args = resolveArgs{
			ctx:                   r.ctx,
			journal:               r.journal,
			mayProxy:              false,
			requirePrimary:        false,
			requireFullAssignment: false,
			minEtcdRevision:       minRevision,
			proxyHeader:           nil,
		}
		var res resolution
		var err error

		select {
		case _ = <-refreshTimer.C:
			goto RefreshFragments

		case _, ok := <-r.maintenanceCh:
			if !ok {
				shutDownReplica(r)
				return
			}
			goto CheckHealth

		case _ = <-pingTicker.C:
			goto CheckHealth
		}

	RefreshFragments:
		if res, err = svc.resolver.resolve(args); err != nil {
			panic(err) // Cannot error, as we control context cancellation.
		} else if res.status != pb.Status_OK {
			// Probable shutdown race (eg, we'll next read that |maintenanceCh| is closed).
			log.WithFields(log.Fields{"status": res.status, "journal": r.journal}).
				Warn("refreshing fragments: failed to resolve")
			continue
		}

		// Begin a background refresh of remote replica fragments. When done,
		// signal to restart |refreshTimer| with the current refresh interval.
		go func(r *replica, spec *pb.JournalSpec) {
			if set, err := fragment.WalkAllStores(r.ctx, spec.Name, spec.Fragment.Stores); err == nil {
				r.index.ReplaceRemote(set)
			} else {
				log.WithFields(log.Fields{
					"name":     spec.Name,
					"err":      err,
					"interval": spec.Fragment.RefreshInterval,
				}).Warn("failed to refresh remote fragments (will retry)")
			}
			refreshTimer.Reset(spec.Fragment.RefreshInterval)
		}(res.replica, res.journalSpec)
		continue

	CheckHealth:
		args.requirePrimary = true
		args.requireFullAssignment = true

		if res, err = svc.resolver.resolve(args); err != nil {
			panic(err) // Cannot error, as we control context cancellation.
		} else if res.status == pb.Status_NOT_JOURNAL_PRIMARY_BROKER {
			// Only current primary checks pipeline health.
		} else if minRevision, err = checkHealth(res, svc.jc, svc.etcd); err != nil {
			log.WithFields(log.Fields{"err": err, "journal": r.journal}).
				Warn("pipeline health check failed (will retry)")
		}
		continue
	}
}

func addTrace(ctx context.Context, format string, args ...interface{}) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf(format, args...)
	}
}

// instrumentJournalServerOp measures and reports the response time of
// |JournalServer| endpoints tagged by operation name and status (success or
// failure). This is typically used with a defer statement.
//
// Example Usage:
//
//  defer instrumentJournalServerOp("append", &err, time.Now())
func instrumentJournalServerOp(op string, err *error, start time.Time) {
	var elapsed = time.Since(start)
	var status = metrics.Fail
	if err == nil || *err == nil {
		status = metrics.Ok
	}

	metrics.JournalServerResponseTimeSeconds.
		WithLabelValues(op, status).
		Observe(float64(elapsed / time.Second))
}

var healthCheckInterval = time.Minute
