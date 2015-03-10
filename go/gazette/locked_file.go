package gazette

import (
	"os"
)

type lockedFile interface {
	// Returns the locked file.
	File() *os.File
	// Closes file and releases held file lock.
	Close() error
}
