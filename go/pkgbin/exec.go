// pkgbin implements functionality related to Flow's packaging,
// including finding binaries and artifcats which are part of
// a packaged Flow release.
package pkgbin

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

// Locate the full path of a binary by:
// * Looking first for a binary that exist alongside the currently
//   executing binary, in a common directory.
// * Only if that's not found, then attempt to find a binary on the $PATH.
func Locate(binary string) (string, error) {
	var execPath, err = os.Executable()
	if err != nil {
		return "", fmt.Errorf("fetching path of current executable: %w", err)
	}

	var path = filepath.Join(filepath.Dir(execPath), binary)
	if _, err = os.Stat(path); err == nil {
		return path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat-ing path %s: %w", path, err)
	}

	// Fall back to looking for |binary| on the path.
	return exec.LookPath(binary)
}

// MustLocate locates the full path of a binary, or panics if it's not found.
func MustLocate(binary string) string {
	var path, err = Locate(binary)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"binary": binary,
			"err":    err,
		}).Fatal("failed to locate required binary")
	}
	return path
}
