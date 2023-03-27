package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/signal"
	"reflect"
	"syscall"
	"unsafe"

	"github.com/estuary/flow/go/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TaskService struct {
	c     *C.TaskService
	conn  *grpc.ClientConn
	lwaCh <-chan struct{}
}

func NewTaskService(
	config pr.TaskServiceConfig,
	publisher ops.Publisher,
) (*TaskService, error) {

	// We must ignore SIGPIPE!
	//
	// This implementation uses a graceful shutdown where it will block on Drop
	// waiting for all client RPCs to complete. HOWEVER, Rust's hyper crate will
	// *NOT* wait for the complete shutdown of any underlying transports,
	// and will immediately close their descriptors. This can cause EPIPE errors
	// when the HTTP/2 transport coroutines attempt reads or writes over those
	// transports -- both from Go and also from Rust.
	//
	// So, we must mask SIGPIPE so that these become EPIPE errno results, which
	// both Go and Rust handle reasonably.
	signal.Ignore(syscall.SIGPIPE)

	var logReader, wDescriptor, err = Pipe()
	if err != nil {
		return nil, fmt.Errorf("creating logging pipe: %w", err)
	}
	config.LogFileFd = int32(wDescriptor)

	// Rust services produce canonical JSON encodings of ops::Log into `wDescriptor`.
	// Parse each and pass to our `publisher`.
	var lwaCh = make(chan struct{})
	go func() {
		defer close(lwaCh)
		var _, err = io.Copy(ops.NewLogWriteAdapter(publisher), logReader)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error":   err,
				"service": config.TaskName,
				"labels":  publisher.Labels(),
			}).Error("failed to process cgo service channel logs")
		}
	}()

	configBytes, err := config.Marshal()
	if err != nil {
		return nil, err
	}
	var h = (*reflect.SliceHeader)(unsafe.Pointer(&configBytes))

	var svc = &TaskService{
		c: C.new_task_service(
			(*C.uint8_t)(unsafe.Pointer(h.Data)),
			C.uint32_t(h.Len),
		),
		lwaCh: lwaCh,
	}

	if err := svc.err(); err != nil {
		svc.Drop()
		return nil, err
	}

	svc.conn, err = grpc.DialContext(
		context.Background(),
		"unix://"+config.UdsPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Instrument client for gRPC metric collection.
		grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	)

	if err != nil {
		svc.Drop()
		return nil, err
	}

	return svc, nil
}

func (s *TaskService) Conn() *grpc.ClientConn {
	return s.conn
}

func (s *TaskService) Drop() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	if s.c != nil {
		C.task_service_drop(s.c)
		s.c = nil
	}
	if s.lwaCh != nil {
		// Block until log read loop reads error or EOF.
		// This happens only after all Rust references of the Pipe have been
		// dropped and the descriptor has been closed.
		<-s.lwaCh
		s.lwaCh = nil
	}
}

func (s *TaskService) err() error {
	var err error
	if s.c.err_len != 0 {
		err = errors.New(C.GoStringN(
			(*C.char)(unsafe.Pointer(s.c.err_ptr)),
			C.int(s.c.err_len)))
	}
	return err
}
