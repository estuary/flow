package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"fmt"
	"io/ioutil"
	"net/http"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// CatalogJSONSchema returns the source catalog JSON schema understood by Flow.
func CatalogJSONSchema() string {
	var svc = newBuildSvc()
	defer svc.finalize()

	svc.sendBytes(uint32(pf.BuildAPI_CATALOG_SCHEMA), nil)

	var _, out, err = svc.poll()
	if err != nil {
		panic(err)
	} else if len(out) != 1 {
		panic("expected 1 output message")
	}
	return string(svc.arenaSlice(out[0]))
}

// BuildCatalog runs the configured build. The provide |client| is used to resolve resource
// fetches.
func BuildCatalog(config pf.BuildAPI_Config, client *http.Client) (hadUserErrors bool, _ error) {
	var svc = newBuildSvc()
	defer svc.finalize()

	if err := svc.sendMessage(uint32(pf.BuildAPI_BEGIN), &config); err != nil {
		panic(err) // Cannot fail to marshal.
	}

	// resolvedCh coordinates resolved work being sent back to the service,
	// in the form of function closures which are executed in serial order
	// against the *service instance.
	var resolvedCh = make(chan func(*service), 8)
	// mayPoll tracks whether we've completed work since our last poll.
	var mayPoll = true

	for {
		var resolveFn func(*service)

		if !mayPoll {
			resolveFn = <-resolvedCh // Must block.
		} else {
			select {
			case resolveFn = <-resolvedCh:
			default: // Don't block.
			}
		}

		if resolveFn != nil {
			resolveFn(svc)
			mayPoll = true
			continue
		}

		// Poll the service.
		svc.sendBytes(uint32(pf.BuildAPI_POLL), nil)
		var _, out, err = svc.poll()
		if err != nil {
			return true, err
		}
		// We must resolve pending work before polling again.
		mayPoll = false

		for _, o := range out {
			switch pf.BuildAPI_Code(o.code) {

			case pf.BuildAPI_DONE:
				return false, nil

			case pf.BuildAPI_DONE_WITH_ERRORS:
				return true, nil

			case pf.BuildAPI_FETCH_REQUEST:
				var fetch pf.BuildAPI_Fetch
				svc.arenaDecode(o, &fetch)

				log.WithField("url", fetch.ResourceUrl).Debug("fetch requested")
				go func() {
					var resp, err = client.Get(fetch.ResourceUrl)
					var data []byte

					if err == nil {
						data, err = ioutil.ReadAll(resp.Body)
					}
					if err == nil && resp.StatusCode != 200 {
						err = fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(data))
					}

					resolvedCh <- func(svc *service) {
						// Write resolution header, followed by result (body or error).
						svc.sendBytes(uint32(pf.BuildAPI_FETCH_REQUEST), []byte(fetch.ResourceUrl))
						if err == nil {
							svc.sendBytes(uint32(pf.BuildAPI_FETCH_SUCCESS), data)
						} else {
							svc.sendBytes(uint32(pf.BuildAPI_FETCH_FAILED), []byte(err.Error()))
						}
					}
				}()

			default:
				log.WithField("code", o.code).Panic("unexpected code from Rust bindings")
			}
		}
	}
}

func newBuildSvc() *service {
	return newService(
		func() *C.Channel { return C.build_create() },
		func(ch *C.Channel, in C.In1) { C.build_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.build_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.build_invoke16(ch, in) },
		func(ch *C.Channel) { C.build_drop(ch) },
	)
}
