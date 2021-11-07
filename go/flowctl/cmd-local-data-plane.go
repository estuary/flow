package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdLocalDataPlane struct {
	BrokerPort   uint16                `long:"broker-port" default:"8080" description:"Port bound by Gazette broker"`
	BuildsRoot   string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	ConsumerPort uint16                `long:"consumer-port" default:"9000" description:"Port bound by Flow consumer"`
	Poll         bool                  `long:"poll" description:"Poll connectors, rather than running them continuously. Required in order to use 'flowctl api poll'"`
	Tempdir      string                `long:"tempdir" description:"Directory for data plane files. If not set, a temporary directory is created and then deleted upon exit"`
	UnixSockets  bool                  `long:"unix-sockets" description:"Bind Gazette to 'gazette.sock' and Flow to 'consumer.sock' within the --tempdir (instead of TCP ports)"`
	Log          mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics  mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`

	etcd     *exec.Cmd
	gazette  *exec.Cmd
	consumer *exec.Cmd
}

func (cmd cmdLocalDataPlane) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	var tempdir = cmd.Tempdir
	var err error

	if tempdir == "" {
		// Create a temporary directory which will contain the Etcd database and various unix:// sockets.
		if tempdir, err = ioutil.TempDir("", "flow-local-data-plane"); err != nil {
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
	fmt.Println("Stopping the local data plane.")

	time.AfterFunc(time.Second, func() {
		fmt.Println("The data plane is taking a while to stop.")
		fmt.Println("Are there still running tasks or collection journals? It blocks until they're deleted.")
		fmt.Println("Or, Ctrl-C again to force it to stop.")

		<-sigCh
		cmd.kill()
	})
	cmd.gracefulStop()

	return nil
}

func (cmd *cmdLocalDataPlane) start(ctx context.Context, tempdir string) (etcdAddr, brokerAddr, consumerAddr string, _ error) {
	var execDir, err = os.Executable()
	if err != nil {
		return "", "", "", fmt.Errorf("getting path of current executable: %w", err)
	}
	execDir = filepath.Dir(execDir)

	// Shell out to start etcd, gazette, and the flow consumer.
	cmd.etcd, etcdAddr = cmd.etcdCmd(ctx, execDir, tempdir)
	cmd.gazette, brokerAddr = cmd.gazetteCmd(ctx, execDir, tempdir, etcdAddr)
	cmd.consumer, consumerAddr = cmd.consumerCmd(ctx, execDir, tempdir, cmd.BuildsRoot, etcdAddr, brokerAddr)

	for _, cmd := range []*exec.Cmd{cmd.etcd, cmd.gazette, cmd.consumer} {
		// Deliver a SIGTERM to the process if this thread should die uncleanly.
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
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

func (cmd *cmdLocalDataPlane) gracefulStop() {
	for _, cmd := range []*exec.Cmd{cmd.consumer, cmd.gazette, cmd.etcd} {
		_ = syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
		_ = cmd.Wait() // Expected to be an error.
	}
}

func (cmd *cmdLocalDataPlane) kill() {
	for _, cmd := range []*exec.Cmd{cmd.consumer, cmd.gazette, cmd.etcd} {
		_ = cmd.Process.Kill()
		_ = cmd.Wait() // Expected to be an error.
	}
}

func (cmd cmdLocalDataPlane) etcdCmd(ctx context.Context, execdir, tempdir string) (*exec.Cmd, string) {
	var out = exec.CommandContext(ctx,
		filepath.Join(execdir, "etcd"),
		"--advertise-client-urls", "unix://client.sock:0",
		"--data-dir", filepath.Join(tempdir, "data-plane.etcd"),
		"--listen-client-urls", "unix://client.sock:0",
		"--listen-peer-urls", "unix://peer.sock:0",
		"--name", "data-plane",
	)
	// The Etcd --log-level flag was added in v3.4. Use it's environment variable
	// version to remain compatible with older `etcd` binaries.
	out.Env = append(out.Env, "ETCD_LOG_LEVEL=error", "ETCD_LOGGER=zap")
	out.Env = append(out.Env, os.Environ()...)

	out.Dir = tempdir
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr

	return out, "unix://" + out.Dir + "/client.sock:0"
}

func (cmd cmdLocalDataPlane) gazetteCmd(ctx context.Context, execdir, tempdir string, etcdAddr string) (*exec.Cmd, string) {
	var addr, port string
	if cmd.UnixSockets {
		port = "unix://localhost" + tempdir + "/gazette.sock"
		addr = port
	} else {
		port = fmt.Sprintf("%d", cmd.BrokerPort)
		addr = "http://localhost:" + port
	}

	var out = exec.CommandContext(ctx,
		filepath.Join(execdir, "gazette"),
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

func (cmd cmdLocalDataPlane) consumerCmd(ctx context.Context, execdir, tempdir, buildsRoot, etcdAddr, gazetteAddr string) (*exec.Cmd, string) {
	var addr, port string
	if cmd.UnixSockets {
		port = "unix://localhost" + tempdir + "/consumer.sock"
		addr = port
	} else {
		port = fmt.Sprintf("%d", cmd.ConsumerPort)
		addr = "http://localhost:" + port
	}

	var args = []string{
		filepath.Join(execdir, "flowctl"),
		"serve",
		"consumer",
		"--broker.address", gazetteAddr,
		"--broker.cache.size", "128",
		"--consumer.limit", "1024",
		"--consumer.max-hot-standbys", "0",
		"--consumer.port", port,
		"--consumer.watch-delay", "0ms", // Speed test execution.
		"--etcd.address", etcdAddr,
		"--flow.builds-root", buildsRoot,
		"--flow.test-apis",
		"--log.format", cmd.Log.Format,
		"--log.level", cmd.Log.Level,
	}
	if cmd.Poll {
		args = append(args, "--flow.poll")
	}

	var out = exec.CommandContext(ctx, args[0], args[1:]...)
	out.Env = append(out.Env, os.Environ()...)
	out.Env = append(out.Env, "TMPDIR="+tempdir)
	out.Dir = tempdir
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr

	return out, addr
}
