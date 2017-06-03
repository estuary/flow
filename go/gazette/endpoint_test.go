package cloudstore

import gc "github.com/go-check/check"

type EndpointSuite struct{}

func (s *EndpointSuite) TestUnmarshalEndpoint(c *gc.C) {
	var data = `{"name": "testep", "type": "s3", "access_key_id": "key"}`
	var ep, err = UnmarshalEndpoint([]byte(data))
	c.Check(err, gc.IsNil)
	var s3ep, ok = ep.(*S3Endpoint)
	if !ok {
		c.Fail()
	}
	c.Check(s3ep.AWSAccessKeyID, gc.Equals, "key")

	data = `{"name": "testep", "type": "sftp", "hostname": "host"}`
	ep, err = UnmarshalEndpoint([]byte(data))
	c.Check(err, gc.IsNil)
	sftpep, ok := ep.(*SFTPEndpoint)
	if !ok {
		c.Fail()
	}
	c.Check(sftpep.SFTPHostname, gc.Equals, "host")

	data = `{"name": "testep", "type": "gcs", "bucket": "testbucket"}`
	ep, err = UnmarshalEndpoint([]byte(data))
	c.Check(err, gc.IsNil)
	gcsep, ok := ep.(*GCSEndpoint)
	if !ok {
		c.Fail()
	}
	c.Check(gcsep.GCSBucket, gc.Equals, "testbucket")
}

var _ = gc.Suite(new(EndpointSuite))
