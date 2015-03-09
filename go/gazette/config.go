package gazette

type Topic struct {
	Name string

	PartitionCount int
	ReplicaCount   int

	// TODO(johnny): Bucket storage options.
}

type Config struct {
	Topics []Topic
}
