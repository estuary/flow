package gazette

import (
	"hash"
	"os"
)

type Spool struct {
	Begin, End  int64
	BackingFile *os.File
	ShaSum      hash.Hash
}
