module github.com/estuary/flow

go 1.14

require (
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/go-openapi/jsonpointer v0.19.3
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.5
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/jessevdk/go-flags v1.4.1-0.20181221193153-c0795c8afcf4
	github.com/jgraettinger/cockroach-encoding v1.1.0
	github.com/lib/pq v1.3.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.5.0
	github.com/stretchr/testify v1.5.1
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200407032746-7eae024eade9
	go.gazette.dev/core v0.88.0
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	google.golang.org/grpc v1.28.0
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20201111163336-72842afa60bf
