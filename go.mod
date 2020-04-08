module github.com/estuary/workspace

go 1.13

require (
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.5.1
	go.gazette.dev/core v0.85.2
	google.golang.org/grpc v1.28.0
)

replace go.gazette.dev/core => ../gazette
