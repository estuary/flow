package cloudstore

import (
	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"

	"github.com/pippio/consensus"
)

type CommonSuite struct {
	keys *consensus.MockKeysAPI
}

func (s *CommonSuite) SetUpSuite(c *gc.C) {
	s.keys = new(consensus.MockKeysAPI)

	var awsEndpoint = &etcd.Response{
		Node: &etcd.Node{Value: testAWSEndpoint},
	}
	s.keys.On("Get", mock.Anything, "/path/to/aws/config", mock.Anything).
		Return(awsEndpoint, nil)

	var sftpEndpoint = &etcd.Response{
		Node: &etcd.Node{Value: testSFTPEndpoint},
	}
	s.keys.On("Get", mock.Anything, "/path/to/sftp/config", mock.Anything).
		Return(sftpEndpoint, nil)
}

func (s *CommonSuite) TestLocationFromAWSEndpoint(c *gc.C) {
	var uri, properties = LocationFromEndpoint(s.keys, "/path/to/aws/config", "")
	c.Check(uri, gc.Equals, "s3://testbucket/testfolder")
	c.Check(properties.Get("AWSAccessKeyID"), gc.Equals, "SOMEACCESSKEY")
	c.Check(properties.Get("AWSSecretAccessKey"), gc.Equals, "SOMESECRET")
}

func (s *CommonSuite) TestLocationFromSFTPEndpoint(c *gc.C) {
	var uri, properties = LocationFromEndpoint(s.keys, "/path/to/sftp/config", "")
	c.Check(uri, gc.Equals, "sftp://localhost/testfolder")
	c.Check(properties.Get("SFTPUsername"), gc.Equals, "testuser")
	c.Check(properties.Get("SFTPPassword"), gc.Equals, "password")
	c.Check(properties.Get("SFTPKeyPath"), gc.Equals, "")
	c.Check(properties.Get("SFTPPort"), gc.Equals, "22")
}

const testAWSEndpoint = `
{
  "s3_bucket": "testbucket",
  "s3_subfolder": "testfolder",
  "aws_secret_access_key": "SOMESECRET",
  "aws_access_key_id": "SOMEACCESSKEY"
}
`

const testSFTPEndpoint = `
{
  "sftp_password": "password",
  "sftp_hostname": "localhost",
  "sftp_directory": "testfolder",
  "sftp_username": "testuser",
  "sftp_port": "22"
}
`

var _ = gc.Suite(new(CommonSuite))
