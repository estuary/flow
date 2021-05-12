package kinesis

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Config represents the fully merged endpoint configuration for Kinesis.
// It matches the `KinesisConfig` struct in `crates/sources/src/specs.rs`
type Config struct {
	Stream             string
	Region             string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
}

func connect(config *Config) (*kinesis.Kinesis, error) {
	var creds = credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecretAccessKey, "")
	var c = aws.NewConfig().WithCredentials(creds).WithRegion(config.Region)

	var awsSession, err = session.NewSession(c)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}
	return kinesis.New(awsSession), nil
}
