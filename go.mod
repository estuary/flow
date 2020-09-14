module github.com/estuary/flow

go 1.14

require (
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.5
	github.com/google/uuid v1.1.1
	github.com/jgraettinger/cockroach-encoding v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.5.0
	github.com/stretchr/testify v1.5.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200407032746-7eae024eade9
	go.gazette.dev/core v0.88.0
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	google.golang.org/grpc v1.28.0
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20200914173619-372d9a4fcfc2
