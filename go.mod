module github.com/estuary/flow

go 1.16

require (
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/estuary/connectors/go-types v0.0.0-20210707160300-897f766d68d4
	github.com/fatih/color v1.7.0
	github.com/gdamore/tcell/v2 v2.3.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.3
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgx/v4 v4.10.1
	github.com/jessevdk/go-flags v1.5.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/minio/highwayhash v1.0.2
	github.com/nsf/jsondiff v0.0.0-20210303162244-6ea32392771e
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/rivo/tview v0.0.0-20210521091241-1fd4a5b7aab3 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/snowflakedb/gosnowflake v1.4.2-0.20210318070613-b0c023e3afd7
	github.com/stretchr/testify v1.7.0
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.etcd.io/etcd/api/v3 v3.5.0-alpha.0
	go.etcd.io/etcd/client/v3 v3.5.0-alpha.0
	go.gazette.dev/core v0.88.0
	golang.org/x/net v0.0.0-20201202161906-c7110b5ffcbb
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.32.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20210710015457-76208a55bce8
