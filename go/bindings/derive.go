package bindings

/*
#include "../../crates/bindings/flow_bindings.h"
#include "rocksdb/c.h"
*/
import "C"

import (
	"encoding/json"
	"unsafe"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/tecbot/gorocksdb"
	pc "go.gazette.dev/core/consumer/protocol"
)

// Derive is an instance of the derivation workflow.
type Derive struct {
	svc       *service
	frames    int
	uuidPtr   string
	fieldPtrs []string
	env       *gorocksdb.Env
}

// NewDerive instantiates the derivation workflow around the given catalog
// and derivation collection name, using the local directory & RocksDB
// environment.
func NewDerive(
	catalogPath string,
	derivation string,
	localDir string,
	uuidPtr string,
	fieldPtrs []string,
	env *gorocksdb.Env,
) (*Derive, error) {
	var svc = newDeriveSvc()

	// gorocksdb.Env has private field, so we must re-interpret to access.
	if unsafe.Sizeof(&gorocksdb.Env{}) != unsafe.Sizeof(&gorocksdbEnv{}) ||
		unsafe.Alignof(&gorocksdb.Env{}) != unsafe.Alignof(&gorocksdbEnv{}) {
		panic("did gorocksdb.Env change? cannot safely reinterpret-cast")
	}
	var innerPtr = uintptr(unsafe.Pointer(((*gorocksdbEnv)(unsafe.Pointer(env))).c))

	svc.mustSendMessage(0, &pf.DeriveAPI_Config{
		CatalogPath:      catalogPath,
		Derivation:       derivation,
		LocalDir:         localDir,
		RocksdbEnvMemptr: uint64(innerPtr),
	})

	if _, _, err := svc.poll(); err != nil {
		return nil, err
	}

	return &Derive{
		svc:       svc,
		frames:    0,
		uuidPtr:   uuidPtr,
		fieldPtrs: fieldPtrs,
		env:       env,
	}, nil
}

type gorocksdbEnv struct {
	c *C.rocksdb_env_t
}

// RestoreCheckpoint returns the last-committed checkpoint in this derivation store.
// It must be called after NewDerive(), before a first transaction is begun.
func (d *Derive) RestoreCheckpoint() (pc.Checkpoint, error) {
	d.svc.sendBytes(1, nil)

	var _, out, err = d.svc.poll()
	if err != nil {
		return pc.Checkpoint{}, err
	}

	var cp pc.Checkpoint
	d.svc.arenaDecode(out[0], &cp)
	return cp, nil
}

// BeginTxn begins a new transaction.
func (d *Derive) BeginTxn() {
	d.svc.sendBytes(2, nil)
	d.frames = 1
}

// Add a document to the current transaction.
func (d *Derive) Add(uuid pf.UUIDParts, key []byte, transformID int32, doc json.RawMessage) error {
	// Send separate "header" vs "body" frames.
	d.svc.mustSendMessage(3, &pf.DeriveAPI_DocHeader{
		Uuid:        &uuid,
		PackedKey:   key,
		TransformId: transformID,
	})
	d.svc.sendBytes(4, doc)
	d.frames += 2

	var err error
	if d.frames%128 == 0 {
		err = d.Flush()
	}
	return err
}

// Flush documents which haven't yet been submitted to the service.
func (d *Derive) Flush() error {
	if _, out, err := d.svc.poll(); err != nil {
		return err
	} else if len(out) != 0 {
		panic("unexpected output frames")
	}
	d.frames = 0
	return nil
}

// Finish deriving documents, invoking the callback for derived document.
func (d *Derive) Finish(cb func(json.RawMessage, []byte, tuple.Tuple) error) error {
	d.svc.mustSendMessage(5, &pf.DeriveAPI_Flush{
		UuidPlaceholderPtr: d.uuidPtr,
		FieldPtrs:          d.fieldPtrs,
	})

	var _, out, err = d.svc.poll()
	if err != nil {
		return err
	} else if err = drainCombineToCallback(d.svc, &out, cb); err != nil {
		return err
	}
	return nil
}

// PrepareCommit persists the current Checkpoint and RocksDB WriteBatch.
func (d *Derive) PrepareCommit(cp pc.Checkpoint) error {
	d.svc.mustSendMessage(6, &pf.DeriveAPI_Prepare{
		Checkpoint: cp,
	})

	if _, _, err := d.svc.poll(); err != nil {
		return err
	}
	return nil
}

// ClearRegisters clears all registers of the Derive service.
// This is a test support function (only), and fails if not run between transactions.
func (d *Derive) ClearRegisters() error {
	d.svc.sendBytes(7, nil)
	return d.Flush()
}

// Stop the DeriveService.
func (d *Derive) Stop() {
	d.svc.finalize()
	d.svc = nil
	d.env.Destroy()
}

func newDeriveSvc() *service {
	return newService(
		func() *C.Channel { return C.derive_create() },
		func(ch *C.Channel, in C.In1) { C.derive_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.derive_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.derive_invoke16(ch, in) },
		func(ch *C.Channel) { C.derive_drop(ch) },
	)
}
