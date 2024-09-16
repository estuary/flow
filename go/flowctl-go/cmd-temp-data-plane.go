package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/estuary/flow/go/pkgbin"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdTempDataPlane struct {
	BrokerPort   uint16                `long:"broker-port" default:"8080" description:"Port bound by Gazette broker"`
	ConsumerPort uint16                `long:"consumer-port" default:"9000" description:"Port bound by Flow consumer"`
	Network      string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Sigterm      bool                  `long:"sigterm" hidden:"true" description:"Send SIGTERM rather than SIGKILL on exit"`
	Tempdir      string                `long:"tempdir" description:"Directory for data plane files. If not set, a temporary directory is created and then deleted upon exit"`
	UnixSockets  bool                  `long:"unix-sockets" description:"Bind Gazette to 'gazette.sock' and Flow to 'consumer.sock' within the --tempdir (instead of TCP ports)"`
	Log          mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics  mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`

	etcd     *exec.Cmd
	gazette  *exec.Cmd
	consumer *exec.Cmd
}

func (cmd cmdTempDataPlane) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	var tempdir = cmd.Tempdir
	var err error

	if tempdir == "" {
		// Create a temporary directory which will contain the Etcd database and various unix:// sockets.
		if tempdir, err = ioutil.TempDir("", "flow-temp-data-plane"); err != nil {
			return fmt.Errorf("creating temp directory: %w", err)
		}
		defer os.RemoveAll(tempdir)
	} else {
		if tempdir, err = filepath.Abs(tempdir); err != nil {
			return fmt.Errorf("--tempdir: %w", err)
		}
	}

	// Install a signal handler which will gracefully stop, and then kill our data plane.
	var sigCh = make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	_, brokerAddr, consumerAddr, err := cmd.start(context.Background(), tempdir)
	if err != nil {
		return fmt.Errorf("starting data plane: %w", err)
	}

	fmt.Printf("export BROKER_ADDRESS=%s\n", brokerAddr)
	fmt.Printf("export CONSUMER_ADDRESS=%s\n", consumerAddr)

	<-sigCh
	fmt.Println("Stopping the temp-data-plane.")

	if cmd.Sigterm {
		time.AfterFunc(time.Second, func() {
			fmt.Println("The data plane is taking a while to stop after SIGTERM.")
			fmt.Println("Ctrl-C again to SIGKILL.")

			<-sigCh
			cmd.kill()
		})
		cmd.gracefulStop()
	} else {
		cmd.kill()
	}

	return nil
}

func (cmd *cmdTempDataPlane) start(ctx context.Context, tempdir string) (etcdAddr, brokerAddr, consumerAddr string, _ error) {
	var buildsRoot = filepath.Join(tempdir, "builds")
	if err := os.Mkdir(buildsRoot, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", "", "", fmt.Errorf("creating builds dir: %w", err)
	}
	buildsRoot = "file://" + buildsRoot + "/"

	// Shell out to start etcd, gazette, and the flow consumer.
	cmd.etcd, etcdAddr = cmd.etcdCmd(ctx, tempdir)
	cmd.gazette, brokerAddr = cmd.gazetteCmd(ctx, tempdir, etcdAddr)
	cmd.consumer, consumerAddr = cmd.consumerCmd(ctx, tempdir, buildsRoot, etcdAddr, brokerAddr)

	for _, cmd := range []*exec.Cmd{cmd.etcd, cmd.gazette, cmd.consumer} {
		// Deliver a SIGTERM to the process if this thread should die uncleanly.
		cmd.SysProcAttr = SysProcAttr()
		// Place child its own process group, so that terminal SIGINT isn't delivered
		// from the terminal and so that we may close leases properly.
		cmd.SysProcAttr.Setpgid = true

		log.WithFields(log.Fields{"args": cmd.Args, "dir": cmd.Dir}).Info("starting command")
		if err := cmd.Start(); err != nil {
			return "", "", "", err
		}
	}
	return etcdAddr, brokerAddr, consumerAddr, nil
}

func (cmd *cmdTempDataPlane) gracefulStop() {
	for _, cmd := range []*exec.Cmd{cmd.consumer, cmd.gazette, cmd.etcd} {
		_ = syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
		_ = cmd.Wait() // Expected to be an error.
	}
}

func (cmd *cmdTempDataPlane) kill() {
	for _, cmd := range []*exec.Cmd{cmd.consumer, cmd.gazette, cmd.etcd} {
		_ = cmd.Process.Kill()
		_ = cmd.Wait() // Expected to be an error.
	}
}

func (cmd cmdTempDataPlane) etcdCmd(ctx context.Context, tempdir string) (*exec.Cmd, string) {
	var out = exec.CommandContext(ctx,
		pkgbin.MustLocate("etcd"),
		"--advertise-client-urls", "unix://client.sock:0",
		"--data-dir", filepath.Join(tempdir, "data-plane.etcd"),
		"--listen-client-urls", "unix://client.sock:0",
		"--listen-peer-urls", "unix://peer.sock:0",
		"--log-level", "error",
		"--logger", "zap",
		"--name", "data-plane",
	)

	out.Dir = tempdir
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr

	return out, "unix://localhost" + out.Dir + "/client.sock:0"
}

func (cmd cmdTempDataPlane) gazetteCmd(ctx context.Context, tempdir string, etcdAddr string) (*exec.Cmd, string) {
	var addr, port string
	if cmd.UnixSockets {
		port = "unix://localhost" + tempdir + "/gazette.sock"
		addr = port
	} else {
		port = fmt.Sprintf("%d", cmd.BrokerPort)
		addr = "http://localhost:" + port
	}

	var out = exec.CommandContext(ctx,
		pkgbin.MustLocate("gazette"),
		"serve",
		"--broker.disable-stores",
		"--broker.max-replication", "1",
		"--broker.port", port,
		"--broker.watch-delay", "0ms", // Speed test execution.
		"--etcd.address", etcdAddr,
		"--log.format", cmd.Log.Format,
		"--log.level", cmd.Log.Level,
	)
	out.Env = append(out.Env, os.Environ()...)
	out.Env = append(out.Env, "TMPDIR="+tempdir)
	out.Dir = tempdir
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr

	return out, addr
}

func (cmd cmdTempDataPlane) consumerCmd(ctx context.Context, tempdir, buildsRoot, etcdAddr, gazetteAddr string) (*exec.Cmd, string) {
	var addr, port string
	if cmd.UnixSockets {
		port = "unix://localhost" + tempdir + "/consumer.sock"
		addr = port
	} else {
		port = fmt.Sprintf("%d", cmd.ConsumerPort)
		addr = "http://localhost:" + port
	}

	var args = []string{
		pkgbin.MustLocate("flowctl-go"),
		"serve",
		"consumer",
		"--broker.address", gazetteAddr,
		"--broker.cache.size", "128",
		"--consumer.allow-origin", "http://localhost:3000",
		"--consumer.limit", "1024",
		"--consumer.max-hot-standbys", "0",
		"--consumer.port", port,
		"--consumer.host", "localhost",
		"--consumer.watch-delay", "0ms", // Speed test execution.
		"--etcd.address", etcdAddr,
		"--flow.builds-root", buildsRoot,
		"--flow.control-api", "http://agent.flow.localhost:8675",
		"--flow.dashboard", "http://dashboard.flow.localhost:3000",
		"--flow.test-apis",
		"--log.format", cmd.Log.Format,
		"--log.level", cmd.Log.Level,
	}
	if cmd.Network != "" {
		args = append(args, "--flow.network", cmd.Network)
	}

	var out = exec.CommandContext(ctx, args[0], args[1:]...)
	out.Env = append(out.Env, os.Environ()...)
	out.Env = append(out.Env, "TMPDIR="+tempdir)
	out.Dir = tempdir
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr

	return out, addr
}
