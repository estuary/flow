package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiLocalDataPlane struct {
	BrokerPort   uint16                `long:"broker-port" default:"8080" description:"Port bound by Gazette broker"`
	BuildsRoot   string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	ConsumerPort uint16                `long:"consumer-port" default:"9000" description:"Port bound by Flow consumer"`
	UnixSockets  bool                  `long:"unix-sockets" description:"Gazette and the Flow consumer should bind Unix domain sockets rather than TCP ports"`
	Log          mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics  mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiLocalDataPlane) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	// Create a temporary directory which will contain the Etcd database and various unix:// sockets.
	tempdir, err := ioutil.TempDir("", "flow-local-data-plane")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tempdir)

	// Install a signal handler which will cancel our context.
	var ctx, cancel = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	_, brokerAddr, consumerAddr, err := cmd.start(ctx, tempdir)
	if err != nil {
		return fmt.Errorf("starting data plane: %w", err)
	}

	fmt.Printf("export BROKER_ADDRESS=%s\n", brokerAddr)
	fmt.Printf("export CONSUMER_ADDRESS=%s\n", consumerAddr)

	<-ctx.Done()
	defer cancel()

	return nil
}

func (cmd apiLocalDataPlane) start(ctx context.Context, tempdir string) (etcdAddr, brokerAddr, consumerAddr string, _ error) {
	// Shell out to start etcd, gazette, and the flow consumer.
	etcdCmd, etcdAddr := cmd.etcdCmd(ctx, tempdir)
	gazetteCmd, brokerAddr := cmd.gazetteCmd(ctx, tempdir, etcdAddr)
	consumerCmd, consumerAddr := cmd.consumerCmd(ctx, tempdir, cmd.BuildsRoot, etcdAddr, brokerAddr)

	for _, cmd := range []*exec.Cmd{etcdCmd, gazetteCmd, consumerCmd} {
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

func (cmd apiLocalDataPlane) etcdCmd(ctx context.Context, tempdir string) (*exec.Cmd, string) {
	var out = exec.CommandContext(ctx,
		"etcd",
		"--listen-peer-urls", "unix://peer.sock:0",
		"--listen-client-urls", "unix://client.sock:0",
		"--advertise-client-urls", "unix://client.sock:0",
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

func (cmd apiLocalDataPlane) gazetteCmd(ctx context.Context, tempdir string, etcdAddr string) (*exec.Cmd, string) {
	var addr, port string
	if cmd.UnixSockets {
		port = "unix://localhost" + tempdir + "/gazette.sock"
		addr = port
	} else {
		port = fmt.Sprintf("%d", cmd.BrokerPort)
		addr = "http://localhost:" + port
	}

	var out = exec.CommandContext(ctx,
		"gazette",
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

func (cmd apiLocalDataPlane) consumerCmd(ctx context.Context, tempdir, buildsRoot, etcdAddr, gazetteAddr string) (*exec.Cmd, string) {
	var addr, port string
	if cmd.UnixSockets {
		port = "unix://localhost" + tempdir + "/consumer.sock"
		addr = port
	} else {
		port = fmt.Sprintf("%d", cmd.ConsumerPort)
		addr = "http://localhost:" + port
	}

	var out = exec.CommandContext(ctx,
		"flowctl",
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
	)
	out.Env = append(out.Env, os.Environ()...)
	out.Env = append(out.Env, "TMPDIR="+tempdir)
	out.Dir = tempdir
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr

	return out, addr
}
