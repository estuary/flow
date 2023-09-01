package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/estuary/flow/go/pkgbin"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// BuildArgs are arguments of the BuildCatalog function.
type BuildArgs struct {
	pf.BuildAPI_Config
	// Context of asynchronous tasks undertaken during the build.
	Context context.Context
	// Directory which roots fetched file:// resolutions.
	// Or empty, if file:// resolutions are disallowed.
	FileRoot string
}

// BuildCatalog runs the configured build.
func BuildCatalog(args BuildArgs) error {
	if err := args.BuildAPI_Config.Validate(); err != nil {
		return fmt.Errorf("validating configuration: %w", err)
	} else if args.FileRoot == "" {
		return fmt.Errorf("FileRoot is required")
	}

	var flowctl, err = pkgbin.Locate("flowctl")
	if err != nil {
		return fmt.Errorf("finding flowctl binary: %w", err)
	}

	// A number of existing Go tests use a relative FileRoot
	// which must be evaluated against the current working directory
	// to resolve to an absolute path, as required by `flowctl`.
	if !path.IsAbs(args.FileRoot) {
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("getting current working directory: %w", err)
		}
		args.FileRoot = path.Join(cwd, args.FileRoot)
	}

	var v = []string{
		"raw",
		"build",
		"--build-id", args.BuildId,
		"--db-path", args.BuildDb,
		"--connector-network", args.ConnectorNetwork,
		"--file-root", args.FileRoot,
		"--source", args.Source,
	}

	var cmd = exec.Command(flowctl, v...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, os.Stdout, os.Stderr

	if err = cmd.Run(); err != nil {
		return fmt.Errorf("catalog build failed: %w", err)
	}
	return nil
}
