package bindings

/*
#include "../../crates/bindings/flow_bindings.h"
#include "rocksdb/c.h"
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"unsafe"

	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jgraettinger/gorocksdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	store_rocksdb "go.gazette.dev/core/consumer/store-rocksdb"
	"go.gazette.dev/core/message"
	"golang.org/x/net/trace"
)

// These metrics give us a sense of update lambda invocation times for the whole process. The idea
// is to have some level of observability to this that doesn't require materializing the ops
// collections, for the sake of resiliency.
var deriveLambdaDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "flow_derive_lambda_duration_seconds",
	Help:    "Duration in seconds of invocation of derive lambdas",
	Buckets: []float64{0.0005, 0.002, 0.01, 0.05, 0.1, 0.3, 1.0},
}, []string{"lambdaType"})

// Derive is an instance of the derivation workflow.
type Derive struct {
	svc     *service
	metrics combineMetrics

	// Fields which are re-initialized with each reconfiguration.
	runningTasks int               // Number of trampoline tasks.
	trampoline   *trampolineServer // Trampolined lambda invocations.
	trampolineCh <-chan []byte     // Completed trampoline tasks.
}

// NewDerive instantiates the derivation API using the RocksDB environment and local directory.
func NewDerive(recorder *recoverylog.Recorder, localDir string, publisher ops.Publisher) (*Derive, error) {
	var rocksEnv *gorocksdb.Env
	if recorder != nil {
		rocksEnv = store_rocksdb.NewHookedEnv(store_rocksdb.NewRecorder(recorder))
	} else {
		rocksEnv = gorocksdb.NewDefaultEnv()
	}

	// gorocksdb.Env has private field, so we must re-interpret to access.
	if unsafe.Sizeof(&gorocksdb.Env{}) != unsafe.Sizeof(&gorocksdbEnv{}) ||
		unsafe.Alignof(&gorocksdb.Env{}) != unsafe.Alignof(&gorocksdbEnv{}) {
		panic("did gorocksdb.Env change? cannot safely reinterpret-cast")
	}
	var innerPtr = uintptr(unsafe.Pointer(((*gorocksdbEnv)(unsafe.Pointer(rocksEnv))).c))

	var svc, err = newDeriveSvc(publisher)
	if err != nil {
		return nil, err
	}
	svc.mustSendMessage(
		uint32(pf.DeriveAPI_OPEN),
		&pf.DeriveAPI_Open{
			RocksdbEnvMemptr: uint64(innerPtr),
			LocalDir:         localDir,
		})

	// At this point, ownership of |rocksEnv| has passed to the derive service.
	// It cannot be accessed or freed from Go.
	rocksEnv = nil

	if err := pollExpectNoOutput(svc); err != nil {
		svc.destroy()
		return nil, err
	}

	var derive = &Derive{
		svc: svc,

		// Fields updated by Configure:
		runningTasks: 0,
		trampoline:   nil,
		trampolineCh: nil,
	}

	runtime.SetFinalizer(derive, func(d *Derive) {
		d.Destroy()
	})

	return derive, nil
}

type gorocksdbEnv struct {
	c *C.rocksdb_env_t
}

// Configure or re-configure the Derive. It must be called after NewDerive()
// before a transaction is begun.
func (d *Derive) Configure(
	fqn string,
	derivation *pf.DerivationSpec,
	typeScriptClient *http.Client,
) error {
	if d.runningTasks != 0 {
		panic("runningTasks != 0")
	}
	if d.trampoline != nil {
		d.trampoline.stop()
	}

	var collection = derivation.Collection.Collection
	combineConfigureCounter.WithLabelValues(fqn, collection.String()).Inc()
	d.metrics = newCombineMetrics(fqn, collection)

	d.trampoline, d.trampolineCh = newTrampolineServer(
		context.Background(),
		newDeriveInvokeHandler(fqn, derivation, typeScriptClient),
	)

	d.svc.mustSendMessage(
		uint32(pf.DeriveAPI_CONFIGURE),
		&pf.DeriveAPI_Config{
			Derivation: derivation,
		})

	return pollExpectNoOutput(d.svc)
}

// RestoreCheckpoint returns the last-committed checkpoint in this derivation store.
// It must be called in between transactions.
func (d *Derive) RestoreCheckpoint() (pc.Checkpoint, error) {
	if d.runningTasks != 0 {
		panic("runningTasks != 0")
	}

	d.svc.sendBytes(uint32(pf.DeriveAPI_RESTORE_CHECKPOINT), nil)

	var _, out, err = d.svc.poll()
	if err != nil {
		return pc.Checkpoint{}, err
	} else if len(out) != 1 || pf.DeriveAPI_Code(out[0].code) != pf.DeriveAPI_RESTORE_CHECKPOINT {
		panic(fmt.Sprintf("unexpected output frames %#v", out))
	}

	var cp pc.Checkpoint
	d.svc.arenaDecode(out[0], &cp)
	return cp, nil
}

// BeginTxn begins a new transaction.
func (d *Derive) BeginTxn() {
	d.svc.sendBytes(uint32(pf.DeriveAPI_BEGIN_TRANSACTION), nil)
}

// Add a document to the current transaction.
func (d *Derive) Add(uuid pf.UUIDParts, key []byte, transformIndex uint32, doc json.RawMessage) error {
	// Send separate "header" vs "body" frames.
	d.svc.mustSendMessage(
		uint32(pf.DeriveAPI_NEXT_DOCUMENT_HEADER),
		&pf.DeriveAPI_DocHeader{
			Uuid:           &uuid,
			PackedKey:      key,
			TransformIndex: transformIndex,
		})
	d.svc.sendBytes(uint32(pf.DeriveAPI_NEXT_DOCUMENT_BODY), doc)

	// If we have no resolved tasks to send, AND we don't have many unsent
	// frames, AND it's not an ACK, THEN skip polling.
	if !d.sendResolvedTasks() &&
		d.svc.queuedFrames() < 128 &&
		message.Flags(uuid.ProducerAndFlags)&message.Flag_ACK_TXN == 0 {
		return nil
	}

	var _, out, err = d.svc.poll()
	if err != nil {
		return err
	}
	d.readTaskStarts(out)

	return err
}

func (d *Derive) readTaskStarts(out []C.Out) {
	for _, o := range out {
		if pf.DeriveAPI_Code(o.code) == pf.DeriveAPI_TRAMPOLINE {
			d.trampoline.startTask(d.svc.arenaSlice(o))
			d.runningTasks++
		} else {
			panic(fmt.Sprintf("unexpected output %#v", o))
		}
	}
}

func (d *Derive) sendResolvedTasks() (sent bool) {
	for {
		select {
		case resolved := <-d.trampolineCh:
			d.svc.sendBytes(uint32(pf.DeriveAPI_TRAMPOLINE), resolved)
			d.runningTasks--
			sent = true
		default:
			return
		}
	}
}

// Drain derived documents, invoking the callback for each distinct group-by document.
func (d *Derive) Drain(cb CombineCallback) (*pf.DeriveAPI_Stats, error) {
	d.svc.sendBytes(uint32(pf.DeriveAPI_FLUSH_TRANSACTION), nil)

	for {
		d.sendResolvedTasks()

		var _, out, err = d.svc.poll()
		if err != nil {
			return nil, err
		}

		log.WithFields(log.Fields{
			"out":   len(out),
			"tasks": d.runningTasks,
		}).Trace("derive.Drain completed poll")

		if len(out) != 0 && pf.DeriveAPI_Code(out[0].code) == pf.DeriveAPI_FLUSHED_TRANSACTION {
			if d.runningTasks != 0 {
				panic(fmt.Sprintf("read FLUSHED_TRANSACTION but d.runningTasks != 0 (is %d)", d.runningTasks))
			} else if len(out) != 1 {
				panic(fmt.Sprintf("read FLUSHED_TRANSACTION but len(out) != 1 (is %d)", len(out)))
			} else {
				break // Loop termination: completed flush and ready to drain.
			}
		}

		// Otherwise we have active tasks, or the first |out| is a task start.
		// We expect remaining |out|'s to be only other task starts.
		d.readTaskStarts(out)

		// We must block until a task is resolved, before polling again.
		if d.runningTasks == 0 {
			panic("d.tasks must be > 0")
		}
		d.svc.sendBytes(uint32(pf.DeriveAPI_TRAMPOLINE), <-d.trampolineCh)
		d.runningTasks--

		log.WithFields(log.Fields{
			"out":   len(out),
			"tasks": d.runningTasks,
		}).Trace("derive.Drain resolved a blocking task")
	}
	log.Trace("derive.Drain completed flush")

	var stats = new(pf.DeriveAPI_Stats)
	var err = drainCombineToCallback(d.svc, cb, stats)

	if err == nil {
		d.recordDeriveDrain(stats)
	}
	return stats, err
}

func (d *Derive) recordDeriveDrain(stats *pf.DeriveAPI_Stats) {
	for _, tf := range stats.Transforms {
		d.metrics.rightDocs.Add(float64(tf.Publish.Output.Docs))
		d.metrics.rightBytes.Add(float64(tf.Publish.Output.Bytes))
	}
	d.metrics.drainDocs.Add(float64(stats.Output.Docs))
	d.metrics.drainBytes.Add(float64(stats.Output.Bytes))
	d.metrics.drainCounter.Inc()
}

// PrepareCommit persists the current Checkpoint and RocksDB WriteBatch.
func (d *Derive) PrepareCommit(cp pc.Checkpoint) error {
	d.svc.mustSendMessage(
		uint32(pf.DeriveAPI_PREPARE_TO_COMMIT),
		&pf.DeriveAPI_Prepare{Checkpoint: cp})
	return pollExpectNoOutput(d.svc)
}

// ClearRegisters clears all registers of the Derive service.
// This is a test support function (only), and fails if not run between transactions.
func (d *Derive) ClearRegisters() error {
	d.svc.sendBytes(uint32(pf.DeriveAPI_CLEAR_REGISTERS), nil)
	return pollExpectNoOutput(d.svc)
}

// Destroy the Derive service, releasing all held resources.
// Destroy may be called when it's known that a *Derive is no longer needed,
// but is optional. If not called explicitly, it will be run during garbage
// collection of the *Derive.
func (d *Derive) Destroy() {
	if d.trampoline != nil {
		// We must stop the trampoline server before |d.svc| may be destroyed,
		// to ensure that no trampoline tasks are reading memory owned by |d.svc|.
		d.trampoline.stop()
		d.trampoline = nil
	}
	if d.svc != nil {
		d.svc.destroy()
		d.svc = nil
	}
}

func newDeriveSvc(publisher ops.Publisher) (*service, error) {
	return newService(
		"derive",
		func(logFilter, logDest C.int32_t) *C.Channel { return C.derive_create(logFilter, logDest) },
		func(ch *C.Channel, in C.In1) { C.derive_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.derive_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.derive_invoke16(ch, in) },
		func(ch *C.Channel) { C.derive_drop(ch) },
		publisher,
	)
}

func newDeriveInvokeHandler(shardFqn string, derivation *pf.DerivationSpec, tsClient *http.Client) trampolineHandler {
	// Decode a trampoline invocation request message.
	var decode = func(request []byte) (interface{}, error) {
		var invoke = new(pf.DeriveAPI_Invoke)
		var err = invoke.Unmarshal(request)
		return invoke, err
	}

	// newRequest builds and returns an idempotent, retry-able request of the lambda,
	// along with the client to which is may be dispatched.
	var newRequest = func(
		ctx context.Context,
		invoke *pf.DeriveAPI_Invoke,
		lambda *pf.LambdaSpec,
	) (request *http.Request, client *http.Client, err error) {

		var url string
		if lambda.Typescript != "" {
			url = fmt.Sprintf("https://localhost%s", lambda.Typescript)
			client = tsClient
		} else if lambda.Remote != "" {
			url = lambda.Remote
			client = http.DefaultClient
		} else {
			return nil, nil, fmt.Errorf("don't know how to invoke lambda %#v", lambda)
		}

		var lambdaBody = newLambdaBody(invoke)
		request, err = http.NewRequestWithContext(ctx, "POST", url, lambdaBody)
		if err != nil {
			return nil, nil, fmt.Errorf("building request for %s: %w", url, err)
		}
		request.ContentLength = int64(lambdaBody.contentLength())

		// Setting both "Idempotency-Key" and GetBody make the request
		// automatically retryable by http package.
		request.GetBody = func() (io.ReadCloser, error) {
			return newLambdaBody(invoke), nil
		}
		request.Header.Add("Idempotency-Key", "")

		return request, client, nil
	}

	// Lookup metrics eagerly, to avoid doing it in a hot loop.
	var updateLambdaTimes = deriveLambdaDurations.WithLabelValues("update")
	var publishLambdaTimes = deriveLambdaDurations.WithLabelValues("publish")

	var exec = func(ctx context.Context, i interface{}) ([]byte, error) {
		var invoke = i.(*pf.DeriveAPI_Invoke)
		// Map from request to applicable transform.
		var transform = &derivation.Transforms[invoke.TransformIndex]

		// Distinguish update vs publish invocations on the presence of registers.
		var lambda *pf.LambdaSpec
		var lambdaType string
		var timer *prometheus.Timer

		if invoke.RegistersLength != 0 {
			lambda = transform.PublishLambda
			lambdaType = "publish"
			timer = prometheus.NewTimer(publishLambdaTimes)
		} else {
			lambda = transform.UpdateLambda
			lambdaType = "update"
			timer = prometheus.NewTimer(updateLambdaTimes)
		}
		defer timer.ObserveDuration()

		var tr = trace.New("flow.Lambda", shardFqn)
		// Add additional information lazily. This will only be evaluated when the /debug/requests
		// page is actually rendered.
		tr.LazyPrintf("transform: %s, lambdaType: %s", transform.Transform, lambdaType)
		defer tr.Finish()

		// Build, dispatch, and read request => response.
		request, client, err := newRequest(ctx, invoke, lambda)
		if err != nil {
			return nil, err
		}

		response, err := client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("invoking %s: %w", request.URL, err)
		}

		var body = bytes.NewBuffer(make([]byte, deriveBufferInitial))
		body.Truncate(taskResponseHeader) // Reserve.

		if _, err = io.Copy(body, response.Body); err != nil {
			return nil, fmt.Errorf("reading lambda response: %w", err)
		}
		if response.StatusCode != 200 && response.StatusCode != 204 {
			return nil, fmt.Errorf("unexpected status %d from %s: %s",
				response.StatusCode,
				request.URL,
				strings.TrimSpace(body.String()[taskResponseHeader:]),
			)
		}
		return body.Bytes(), nil
	}

	return trampolineHandler{
		taskCode: uint32(pf.DeriveAPI_TRAMPOLINE_INVOKE),
		decode:   decode,
		exec:     exec,
	}
}

// lambdaBody is an io.Reader suited for use with http.Request.
// It serves Read by splicing from contained buffers, knows how
// to compute its Content-Length, and is also a no-op Closer.
type lambdaBody struct {
	parts [][]byte
}

var bodyOpenBytes = []byte("[[")
var bodyCommaBytes = []byte("],[")
var bodyCloseBytes = []byte("]]")

func newLambdaBody(invoke *pf.DeriveAPI_Invoke) *lambdaBody {
	// Map source and register documents to zero-copy []byte slices.
	// Only set their values if non-empty, as Rust defaults to 0x1
	// for empty slice pointers, and the Go runtime very reasonably
	// panics on encountering an invalid pointer when copying stacks.
	var sources, registers []byte

	if invoke.SourcesLength != 0 {
		var h = (*reflect.SliceHeader)(unsafe.Pointer(&sources))
		h.Cap = int(invoke.SourcesLength)
		h.Len = int(invoke.SourcesLength)
		h.Data = uintptr(invoke.SourcesMemptr)
	} else {
		panic("sources cannot be empty")
	}

	if invoke.RegistersLength != 0 {
		var h = (*reflect.SliceHeader)(unsafe.Pointer(&registers))
		h.Cap = int(invoke.RegistersLength)
		h.Len = int(invoke.RegistersLength)
		h.Data = uintptr(invoke.RegistersMemptr)
	}

	if log.IsLevelEnabled(log.TraceLevel) {
		log.WithFields(log.Fields{
			"sources":   string(sources),
			"registers": string(registers),
		}).Trace("building lambda body")
	}

	if len(registers) == 0 {
		return &lambdaBody{
			parts: [][]byte{
				bodyOpenBytes,
				sources,
				bodyCloseBytes,
			},
		}
	} else {
		return &lambdaBody{
			parts: [][]byte{
				bodyOpenBytes,
				sources,
				bodyCommaBytes,
				registers,
				bodyCloseBytes,
			},
		}
	}
}

func (b *lambdaBody) Read(p []byte) (n int, err error) {
	for len(b.parts) != 0 {
		var nn = copy(p[n:], b.parts[0]) // Fill [n:] onward.
		b.parts[0] = b.parts[0][nn:]     // Consume |nn| of part.
		n += nn                          // Total read |n| includes |nn|.

		if len(b.parts[0]) == 0 {
			b.parts = b.parts[1:] // Part is consumed.
		} else if len(p) == n {
			return
		} else {
			panic("n != len(p) but data remains in buffer part")
		}
	}
	err = io.EOF
	return
}

func (b *lambdaBody) contentLength() (n int64) {
	for _, p := range b.parts {
		n += int64(len(p))
	}
	return
}

func (b *lambdaBody) Close() error { return nil }

// Use 64K initial read buffer, matching the target
// buffer size of derive pipeline blocks.
// This is also the initial HTTP/2 flow control window.
const deriveBufferInitial = 1 << 16
