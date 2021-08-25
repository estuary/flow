module github.com/estuary/protocols

go 1.16

require (
	github.com/alecthomas/jsonschema v0.0.0-20210818095345-1014919a589c
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/gogo/protobuf v1.3.2
	github.com/google/uuid v1.3.0
	github.com/jessevdk/go-flags v1.5.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/minio/highwayhash v1.0.2
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	go.gazette.dev/core v0.88.0
	google.golang.org/grpc v1.40.0
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20210726192503-178f10d4ba3d
