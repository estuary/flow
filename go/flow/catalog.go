package flow

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strconv"

	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/broker/protocol"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
)

const (
	// TasksPrefix prefixes CatalogTasks.
	TasksPrefix = "/tasks/"
	// CommonsPrefix prefixes CatalogCommons.
	CommonsPrefix = "/commons/"
)

// Catalog is a type wrapper of a KeySpace that's a local mirror of
// catalog entities across the cluster.
type Catalog struct {
	*keyspace.KeySpace
}

// NewCatalog builds and loads a KeySpace and Catalog which load, decode, and watch Flow catalog entities.
func NewCatalog(ctx context.Context, etcd *clientv3.Client, root string) (Catalog, error) {
	if root != path.Clean(root) {
		return Catalog{}, fmt.Errorf("%q is not a clean path", root)
	}

	var (
		tasksPrefix   = root + TasksPrefix
		commonsPrefix = root + CommonsPrefix
	)

	var decoder = func(raw *mvccpb.KeyValue) (interface{}, error) {
		var m interface {
			Unmarshal([]byte) error
			Validate() error
		}

		switch {
		case bytes.HasPrefix(raw.Key, []byte(tasksPrefix)):
			m = new(pf.CatalogTask)
		case bytes.HasPrefix(raw.Key, []byte(commonsPrefix)):
			m = new(Commons)
			runtime.SetFinalizer(m, func(c *Commons) { c.Destroy() })
		default:
			return nil, fmt.Errorf("unexpected key prefix")
		}

		if err := m.Unmarshal(raw.Value); err != nil {
			return nil, fmt.Errorf("decoding %q: %w", string(raw.Key), err)
		} else if err = m.Validate(); err != nil {
			return nil, fmt.Errorf("validating %q: %w", string(raw.Key), err)
		}

		// Sanity-check that Etcd key and computed value suffix agree.
		var expect string
		var actual []byte

		switch {
		case bytes.HasPrefix(raw.Key, []byte(tasksPrefix)):
			expect = m.(*pf.CatalogTask).Name()
			actual = raw.Key[len(tasksPrefix):]

			log.WithFields(log.Fields{
				"name":           expect,
				"createRevision": raw.CreateRevision,
				"modRevision":    raw.ModRevision,
			}).Debug("decoded CatalogTask")

		case bytes.HasPrefix(raw.Key, []byte(commonsPrefix)):
			expect = m.(*Commons).CommonsId
			actual = raw.Key[len(commonsPrefix):]

			log.WithFields(log.Fields{
				"name":           expect,
				"createRevision": raw.CreateRevision,
				"modRevision":    raw.ModRevision,
			}).Debug("decoded CatalogCommons")
		}

		if expect != string(actual) {
			return nil, fmt.Errorf("etcd key %q has a different computed key, %q",
				string(raw.Key), expect)
		}

		return m, nil
	}

	var catalog = Catalog{
		KeySpace: keyspace.NewKeySpace(root, decoder),
	}

	if err := catalog.Load(ctx, etcd, 0); err != nil {
		return Catalog{}, fmt.Errorf("initial load of %q: %w", root, err)
	}
	return catalog, nil
}

// GetTask returns the named CatalogTask, Commons, and its Commons ModRevision. It first ensures
// that the given revision has been observed by the keyspace so that the caller can guarantee that a
// task update has been observed if its revision is known.
func (c Catalog) GetTask(ctx context.Context, name string, taskCreated string) (_ *pf.CatalogTask, _ *Commons, revision int64, _ error) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	if taskCreated != "" {
		var waitForRevision, err = strconv.ParseInt(taskCreated, 10, 64)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("parsing task-created label: %w", err)
		}
		c.KeySpace.WaitForRevision(ctx, waitForRevision)
	}
	return c.getTask(name)
}

// GetCommons returns the identified Commons, and its ModRevision.
func (c Catalog) GetCommons(id string) (_ *Commons, revision int64, _ error) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	return c.getCommons(id)
}

func (c Catalog) getTask(name string) (task *pf.CatalogTask, commons *Commons, revision int64, err error) {
	var ind, found = c.Search(c.Root + TasksPrefix + name)
	if !found {
		return nil, nil, 0, fmt.Errorf("catalog task %q not found", name)
	}

	task, revision = c.KeyValues[ind].Decoded.(*pf.CatalogTask), c.KeyValues[ind].Raw.ModRevision
	commons, _, err = c.getCommons(task.CommonsId)
	return
}

func (c Catalog) getCommons(id string) (commons *Commons, revision int64, err error) {
	var ind, found = c.Search(c.Root + CommonsPrefix + id)
	if !found {
		return nil, 0, fmt.Errorf("catalog commons %q not found", id)
	}
	return c.KeyValues[ind].Decoded.(*Commons), c.KeyValues[ind].Raw.ModRevision, nil
}

// SignalOnTaskUpdate signals the callback |cb| if the given named task
// and last-observed revision is either updated or removed.
func (c Catalog) SignalOnTaskUpdate(ctx context.Context, name string, revision int64, cb func()) {
	// TODO(johnny): Consider using KeySpace.Observers to maintain a consolidated
	// index rather than spawning off a bunch of goroutines.
	go func() {
		defer cb()

		c.KeySpace.Mu.RLock()
		defer c.KeySpace.Mu.RUnlock()

		for {
			// Note |next| is 0 if task |name| doesn't exist.
			if _, _, next, _ := c.getTask(name); revision != next {
				return
			} else if err := c.KeySpace.WaitForRevision(ctx, c.KeySpace.Header.Revision+1); err != nil {
				return
			}
		}
	}()
}

// AllTasks returns a slice of all CatalogTasks.
func (c Catalog) AllTasks() []*pf.CatalogTask {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	var tasks = c.Prefixed(c.Root + TasksPrefix)
	var out = make([]*pf.CatalogTask, len(tasks))

	for i, kv := range tasks {
		out[i] = kv.Decoded.(*pf.CatalogTask)
	}
	return out
}

// ApplyArgs are arguments to ApplyCatalogToEtcd.
type ApplyArgs struct {
	Ctx  context.Context
	Etcd *clientv3.Client
	// Root of the catalog keyspace in Etcd.
	Root string
	// BuiltCatalog to apply.
	Build *catalog.BuiltCatalog
	// TypeScriptUDS is a Unix domain socket at which the catalog's TypeScript
	// runtime can be reached. If empty, TypeScriptPackageURL must be set.
	TypeScriptUDS string
	// TypeScriptPackageURL is a URL at which the catalog's TypeScript package
	// may be found. If empty, TypeScriptUDS must be set.
	TypeScriptPackageURL string
	// Prepare all apply actions without actually running them.
	DryRun bool
	// Prune entities in Etcd which aren't in Build.
	Prune bool
}

// ApplyCatalogToEtcd inserts a CatalogCommons and updates CatalogTasks
// into the Etcd Catalog keyspace rooted by |root|.
// It returns the generated Commons ID and revision.
func ApplyCatalogToEtcd(args ApplyArgs) (string, int64, error) {
	if args.TypeScriptUDS == "" && args.TypeScriptPackageURL == "" {
		return "", 0, fmt.Errorf("expected a TypeScript UDS or package")
	}

	var oldCatalog, err = NewCatalog(args.Ctx, args.Etcd, args.Root)
	if err != nil {
		return "", 0, fmt.Errorf("loading existing catalog: %w", err)
	}
	var oldKeys = make(map[string]int64, len(oldCatalog.KeyValues))
	for _, kv := range oldCatalog.KeyValues {
		oldKeys[string(kv.Raw.Key)] = kv.Raw.ModRevision
	}
	var build = args.Build

	// Build CatalogCommons and CatalogTasks around a generated CommonsID.
	var commons = pf.CatalogCommons{
		CommonsId:             build.UUID.String(),
		Schemas:               build.Schemas,
		TypescriptLocalSocket: args.TypeScriptUDS,
		TypescriptPackageUrl:  args.TypeScriptPackageURL,
	}
	var tasks []pf.CatalogTask

	for i := range build.Captures {
		tasks = append(tasks, pf.CatalogTask{
			CommonsId: commons.CommonsId,
			Capture:   &build.Captures[i],
		})
	}
	var derivations = make(map[pf.Collection]struct{})
	for i := range build.Derivations {
		tasks = append(tasks, pf.CatalogTask{
			CommonsId:  commons.CommonsId,
			Derivation: &build.Derivations[i],
		})
		derivations[build.Derivations[i].Collection.Collection] = struct{}{}
	}
	// Non-derivation collections are ingestion tasks.
	for i := range build.Collections {
		if _, ok := derivations[build.Collections[i].Collection]; ok {
			continue
		}
		tasks = append(tasks, pf.CatalogTask{
			CommonsId: commons.CommonsId,
			Ingestion: &build.Collections[i],
		})
	}
	for i := range build.Materializations {
		tasks = append(tasks, pf.CatalogTask{
			CommonsId:       commons.CommonsId,
			Materialization: &build.Materializations[i],
		})
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name() < tasks[j].Name() })

	// Validate the world.
	if err := commons.Validate(); err != nil {
		return "", 0, fmt.Errorf("validating commons: %w", err)
	}
	for t := range tasks {
		if err := tasks[t].Validate(); err != nil {
			return "", 0, fmt.Errorf("validating Tasks[%d]: %w", t, err)
		}
	}

	// Build an Etcd transaction which applies the request tasks & commons.
	var cmps []clientv3.Cmp
	var ops []clientv3.Op

	for _, task := range tasks {
		var key = args.Root + TasksPrefix + task.Name()

		if rev, ok := oldKeys[key]; ok {
			log.WithField("key", key).Info("updating catalog task")
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", rev))
			delete(oldKeys, key)
		} else {
			log.WithField("key", key).Info("inserting catalog task")
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", 0))
		}
		ops = append(ops, clientv3.OpPut(key, marshalString(&task)))
	}
	var key = args.Root + CommonsPrefix + commons.CommonsId
	ops = append(ops, clientv3.OpPut(key, marshalString(&commons)))
	log.WithField("key", key).Debug("inserting catalog commons")

	// If pruning, then delete remaining old keys.
	if args.Prune {
		for key, rev := range oldKeys {
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", rev))
			ops = append(ops, clientv3.OpDelete(key))
			log.WithField("key", key).Info("removing catalog task")
		}
	}

	if args.DryRun {
		return commons.CommonsId, 0, nil
	}

	txnResp, err := args.Etcd.Do(args.Ctx, clientv3.OpTxn(cmps, ops, nil))
	if err == nil && !txnResp.Txn().Succeeded {
		return "", 0, fmt.Errorf("etcd transaction checks failed")
	} else if err != nil {
		return "", 0, err
	}
	return commons.CommonsId, txnResp.Txn().Header.Revision, nil
}

func marshalString(m interface{ Marshal() ([]byte, error) }) string {
	var b, err = m.Marshal()
	if err != nil {
		panic(err) // Cannot fail to marshal.
	}
	return string(b)
}

// OverrideForLocalExecution resets all configured journal and
// shard replication factors to one, and clears append rate limits.
func OverrideForLocalExecution(catalog *catalog.BuiltCatalog) {
	walkCatalog(
		catalog,
		func(s *pb.JournalSpec) {
			s.Replication = 1
			s.MaxAppendRate = 0
		},
		func(s *pc.ShardSpec) {
			s.HotStandbys = 0
		},
	)
}

// OverrideForLocalFragmentStores resets all configured fragment stores
// to instead use the local "file:///" system.
func OverrideForLocalFragmentStores(catalog *catalog.BuiltCatalog) {
	walkCatalog(
		catalog,
		func(s *pb.JournalSpec) { s.Fragment.Stores = []protocol.FragmentStore{"file:///"} },
		func(s *pc.ShardSpec) {},
	)
}

// OverrideForNoFragmentStores resets all configured fragment stores
// to be empty, meaning that fragments are not persisted at all.
// This is only use for in-process testing.
func OverrideForNoFragmentStores(catalog *catalog.BuiltCatalog) {
	walkCatalog(
		catalog,
		func(s *pb.JournalSpec) { s.Fragment.Stores = nil },
		func(s *pc.ShardSpec) {},
	)
}

func walkCatalog(catalog *catalog.BuiltCatalog, onJournal func(*pb.JournalSpec), onShard func(*pc.ShardSpec)) {
	for i := range catalog.Collections {
		onJournal(catalog.Collections[i].PartitionTemplate)
	}

	for i := range catalog.Captures {
		onShard(catalog.Captures[i].ShardTemplate)
		onJournal(catalog.Captures[i].RecoveryLogTemplate)

		// The nested collection is informational only.
		// Failing to set this would have no actual impact.
		for b := range catalog.Captures[i].Bindings {
			onJournal(catalog.Captures[i].Bindings[b].Collection.PartitionTemplate)
		}
	}

	for i := range catalog.Derivations {
		onShard(catalog.Derivations[i].ShardTemplate)
		onJournal(catalog.Derivations[i].RecoveryLogTemplate)

		// As with Captures, this is informational only.
		onJournal(catalog.Derivations[i].Collection.PartitionTemplate)
	}

	for i := range catalog.Materializations {
		onShard(catalog.Materializations[i].ShardTemplate)
		onJournal(catalog.Materializations[i].RecoveryLogTemplate)

		// As with Captures, this is informational only.
		for b := range catalog.Materializations[i].Bindings {
			onJournal(catalog.Materializations[i].Bindings[b].Collection.PartitionTemplate)
		}
	}
}
