package gazette

type Fragment struct {
	Begin, End int64
	SHA1Sum    string

	IsLocal     bool
	IsPersisted bool
}

type FragmentIndex struct {
	JournalName string

	GCSContext *logging.GCSContext

	Fragments []Fragment
}
