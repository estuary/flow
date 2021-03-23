package flow

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"runtime"
	"sort"

	"github.com/estuary/flow/go/bindings"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
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

// GetIngestion returns the named ingestion task.
func (c Catalog) GetIngestion(name string) (*pf.CollectionSpec, *Commons, error) {
	var task, commons, err = c.GetTask(name)
	if err != nil {
		return nil, nil, err
	} else if task.Ingestion == nil {
		return nil, nil, ErrCatalogTaskNotIngestion
	}
	return task.Ingestion, commons, nil
}

// GetDerivation returns the named derivation task.
func (c Catalog) GetDerivation(name string) (*pf.CollectionSpec, *Commons, error) {
	var task, commons, err = c.GetTask(name)
	if err != nil {
		return nil, nil, err
	} else if task.Ingestion == nil {
		return nil, nil, ErrCatalogTaskNotIngestion
	}
	return task.Ingestion, commons, nil
}

// GetTask returns the named CatalogTask.
func (c Catalog) GetTask(name string) (*pf.CatalogTask, *Commons, error) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	var ind, found = c.Search(c.Root + TasksPrefix + name)
	if !found {
		return nil, nil, ErrCatalogTaskNotFound
	}
	var task = c.KeyValues[ind].Decoded.(*pf.CatalogTask)

	ind, found = c.Search(c.Root + CommonsPrefix + task.CommonsId)
	if !found {
		return task, nil, ErrCatalogCommonsNotFound
	}
	var commons = c.KeyValues[ind].Decoded.(*Commons)

	return task, commons, nil
}

// ApplyArgs are arguments to ApplyCatalogToEtcd.
type ApplyArgs struct {
	Ctx                  context.Context
	Etcd                 *clientv3.Client
	Root                 string
	Build                *bindings.BuiltCatalog
	TypeScriptUDS        string
	TypeScriptPackageURL string
	DryRun               bool
}

// ApplyCatalogToEtcd inserts a CatalogCommons and updates CatalogTasks
// into the Etcd Catalog keyspace rooted by |root|.
func ApplyCatalogToEtcd(args ApplyArgs) (int64, error) {
	if args.TypeScriptUDS == "" && args.TypeScriptPackageURL == "" {
		return 0, fmt.Errorf("expected a TypeScript UDS or package")
	}

	var oldCatalog, err = NewCatalog(args.Ctx, args.Etcd, args.Root)
	if err != nil {
		return 0, fmt.Errorf("loading existing catalog: %w", err)
	}
	var oldKeys = make(map[string]int64, len(oldCatalog.KeyValues))
	for _, kv := range oldCatalog.KeyValues {
		oldKeys[string(kv.Raw.Key)] = kv.Raw.ModRevision
	}
	var build = args.Build

	// Build CatalogCommons and CatalogTasks around a generated CommonsID.
	var commons = pf.CatalogCommons{
		CommonsId:             uuid.New().String(),
		JournalRules:          build.JournalRules,
		ShardRules:            build.ShardRules,
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
		return 0, fmt.Errorf("validating commons: %w", err)
	}
	for t := range tasks {
		if err := tasks[t].Validate(); err != nil {
			return 0, fmt.Errorf("validating Tasks[%d]: %w", t, err)
		}
	}

	// Build an Etcd transaction which applies the request tasks & commons.
	var cmps []clientv3.Cmp
	var ops []clientv3.Op

	for _, task := range tasks {
		var key = args.Root + TasksPrefix + task.Name()

		if rev, ok := oldKeys[key]; ok {
			log.WithField("key", key).Debug("updating CatalogTask")
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", rev))
			delete(oldKeys, key)
		} else {
			log.WithField("key", key).Debug("inserting CatalogTask")
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", 0))
		}
		ops = append(ops, clientv3.OpPut(key, marshalString(&task)))
	}
	var key = args.Root + CommonsPrefix + commons.CommonsId
	ops = append(ops, clientv3.OpPut(key, marshalString(&commons)))
	log.WithField("key", key).Debug("inserting CatalogCommons")

	// Delete remaining old keys.
	for key, rev := range oldKeys {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", rev))
		ops = append(ops, clientv3.OpDelete(key))
		log.WithField("key", key).Debug("removing dropped catalog item")
	}

	if args.DryRun {
		return 0, nil
	}

	txnResp, err := args.Etcd.Do(args.Ctx, clientv3.OpTxn(cmps, ops, nil))
	if err == nil && !txnResp.Txn().Succeeded {
		return 0, fmt.Errorf("etcd transaction checks failed")
	} else if err != nil {
		return 0, err
	}
	return txnResp.Txn().Header.Revision, nil
}

func marshalString(m interface{ Marshal() ([]byte, error) }) string {
	var b, err = m.Marshal()
	if err != nil {
		panic(err) // Cannot fail to marshal.
	}
	return string(b)
}

var (
	ErrCatalogTaskNotFound      = fmt.Errorf("not found")
	ErrCatalogCommonsNotFound   = fmt.Errorf("catalog commons not found")
	ErrCatalogTaskNotIngestion  = fmt.Errorf("not an ingestion")
	ErrCatalogTaskNotDerivation = fmt.Errorf("not a derivation")
)
