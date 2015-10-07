package recoverylog

// FileRecord is a record of a file in a file-system.
type FileRecord struct {
	// Stable ID which represents a file independent of renames or links.
	Id string
	// Log offset which is at or before all operations on |file| in the log.
	FirstOffset int64
	// Set of linked file paths in the target filesystem.
	Links map[string]struct{}
}

// Manifest is representation of a file system. It maintains |FileRecord|s and
// corresponding links across creations, deletions, renames, and links.
type Manifest interface {
	// Creates a new FileRecord linked to |fname| with FirstOffset |offset|.
	// |fname| must not already be in use.
	CreateFile(fname string, offset int64) (*FileRecord, error)
	// Deletes a link to |fname| from its owning FileRecord. The link must exist,
	// and the FileRecord is returned on success.
	DeleteFile(fname string) (*FileRecord, error)
	// Moves a link |src| to instead link |target|. The |src| link must exist,
	// the |target| link must not. The modified FileRecord is returned on success.
	RenameFile(src, target string) (*FileRecord, error)
	// Adds a link |target| for the file currently linked to |src|. The |src| link
	// must exist, and |target| must not. The modified FileRecord is returned on
	// success.
	LinkFile(src, target string) (*FileRecord, error)
}
