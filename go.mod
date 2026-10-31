module github.com/unsuitable001/openetl

go 1.26.0

toolchain go1.26.10

require (
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/stoewer/go-strcase v1.3.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sync v0.24.0
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.12
)

// sdk/go is developed in this repo. go.work covers builds, but `go mod tidy`
// ignores workspaces, so the replace keeps tidy and GOWORK=off builds working.
replace github.com/unsuitable001/openetl/sdk/go => ./sdk/go

require (
	connectrpc.com/connect v1.21.1
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/aws/aws-sdk-go-v2 v1.47.5
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.6
	github.com/unsuitable001/openetl/sdk/go v0.0.0-00010101000000-000000000000
	google.golang.org/genproto/googleapis/api v0.0.0-20261012220534-4341edf3f941
	google.golang.org/grpc v1.84.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.8 // indirect
	github.com/aws/smithy-go v1.28.7 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261005182115-fad411399dd8 // indirect
)
