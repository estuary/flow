module github.com/estuary/flow

go 1.16

require (
	github.com/alecthomas/jsonschema v0.0.0-20210818095345-1014919a589c
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/estuary/protocols v0.0.0-20210826160403-70c70a3448e3
	github.com/fatih/color v1.7.0
	github.com/go-openapi/jsonpointer v0.19.3
	github.com/gogo/protobuf v1.3.2
	github.com/google/uuid v1.1.2
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgx/v4 v4.10.1
	github.com/jessevdk/go-flags v1.5.0
	github.com/jgraettinger/gorocksdb v0.0.0-20210726190246-aede64bf0e7b
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/minio/highwayhash v1.0.2
	github.com/nsf/jsondiff v0.0.0-20210303162244-6ea32392771e
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/sirupsen/logrus v1.8.1
	github.com/snowflakedb/gosnowflake v1.4.2-0.20210318070613-b0c023e3afd7
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.gazette.dev/core v0.88.0
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.40.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20210726192503-178f10d4ba3d
