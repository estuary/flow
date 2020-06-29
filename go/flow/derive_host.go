package flow

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"google.golang.org/grpc"
)

type deriveConfig struct {
	// Path to catalog.
	Catalog string
	// Name of collection which we're deriving.
	Derivation string
	// Unix domain socket to listen on.
	SocketPath string
	// FSM which details the persistent state manifest, including its recovery log.
	FSM *recoverylog.FSM
	// Author under which new operations should be fenced and recorded to the log.
	Author recoverylog.Author
	// Directory which roots the persistent state of this worker.
	Dir string
	// Registers to check during recovery-log writes.
	CheckRegisters *pb.LabelSelector
}

func newDeriveConfig(catalog *catalog, derivation string, rec *recoverylog.Recorder) (*deriveConfig, error) {
	return &deriveConfig{
		Catalog:        catalog.LocalPath(),
		Derivation:     derivation,
		SocketPath:     path.Join(rec.Dir, "unix-socket"),
		FSM:            rec.FSM,
		Author:         rec.Author,
		Dir:            rec.Dir,
		CheckRegisters: rec.CheckRegisters,
	}, nil
}

type deriveHost struct {
	cfg    deriveConfig
	cmd    *exec.Cmd
	conn   *grpc.ClientConn
	client pf.DeriveClient
}

func newDeriveHost(cfg deriveConfig) (*deriveHost, error) {
	// Write out derive-worker configuration.
	var cfgFile, err = ioutil.TempFile(cfg.Dir, "derive-config")
	if err != nil {
		return nil, fmt.Errorf("failed to create derive-config: %w", err)
	} else if err = json.NewEncoder(cfgFile).Encode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	var cmd = exec.Command("derive-worker", "--config", cfgFile.Name())
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe for derive-worker: %w", err)
	} else if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start derive-worker: %w", err)
	}
	log.WithField("args", cmd.Args).Info("started derive-worker")

	var br = bufio.NewReader(stdout)
	if ready, err := br.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("failed to read READY from derive-worker: %w", err)
	} else if ready != "READY\n" {
		return nil, fmt.Errorf("unexpected READY from derive-worker: %q", ready)
	}

	conn, err := grpc.DialContext(context.Background(), "unix://"+cfg.SocketPath, grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to dial derive-worker: %w", err)
	}

	return &deriveHost{
		cfg:    cfg,
		cmd:    cmd,
		conn:   conn,
		client: pf.NewDeriveClient(conn),
	}, nil
}

// Stop gracefully stops the derive worker process, and returns an updated
// deriveConfig which may be used to build a future deriveHost.
func (dh *deriveHost) Stop() (*deriveConfig, error) {
	return nil, fmt.Errorf("not implemented yet")
}
