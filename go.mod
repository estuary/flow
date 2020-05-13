module github.com/estuary/proj

go 1.14

require (
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/stretchr/testify v1.5.1
	go.gazette.dev/core v0.86.1
	google.golang.org/grpc v1.28.0
)

replace go.gazette.dev/core => ../gazette
