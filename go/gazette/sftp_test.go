package cloudstore

import (
	"context"
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"github.com/pkg/sftp"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/crypto/ssh"

	"github.com/pippio/consensus"
	"github.com/pippio/endpoints"
)

var hostKeyAlgorithms = []string{"ssh-rsa", "ssh-dss", "ecdsa-sha2-nistp256"}

type SFTPSuite struct {
	cfs                      FileSystem
	keyRSA, keyDSA, keyECDSA ssh.Signer
}

func (s *SFTPSuite) SetUpSuite(c *gc.C) {
	// Skip entirely in Short mode.
	if testing.Short() {
		c.Skip("SFTP tests only run in long mode.")
		return
	}

	endpoints.ParseFromEnvironment()
	flag.Parse()

	// etcd needed for GCS configuration data.
	var etcdClient, err = etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	if err != nil {
		c.Skip("SFTP test can only run with etcd running and populated with GCS fixtures.")
		return
	}
	var keysAPI = etcd.NewKeysAPI(etcdClient)

	properties, err := keysAPI.Get(context.Background(), "/properties",
		&etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		c.Skip("SFTP test can only run with etcd running and populated with GCS fixtures.")
		return
	}
	var etcdProperties = consensus.MapAdapter(properties.Node)

	// Force use of the real cloud filesystem. We can't perform this test with
	// the filesystem-backed fake cloudFS. Note that we can't use ?compress=true,
	// either: see GoogleCloudPlatform/gcsfuse#165.
	s.cfs, err = NewFileSystem(etcdProperties, "gs://")
	c.Assert(err, gc.IsNil)

	keyBytes := []byte(etcdProperties.Get("sftp/SSHHostECDSAKey"))
	s.keyECDSA, err = ssh.ParsePrivateKey(keyBytes)
	c.Assert(err, gc.IsNil)

	keyBytes = []byte(etcdProperties.Get("sftp/SSHHostDSAKey"))
	s.keyDSA, err = ssh.ParsePrivateKey(keyBytes)
	c.Assert(err, gc.IsNil)

	keyBytes = []byte(etcdProperties.Get("sftp/SSHHostRSAKey"))
	s.keyRSA, err = ssh.ParsePrivateKey(keyBytes)
	c.Assert(err, gc.IsNil)
}

// Verifies that a fixture file staged via |cfs| can be read over SFTP,
// NOTE(joshk): If we ever decide that the fixture file should be written to a
// subdirectory, you'll need to call sftpClient.Mkdir("subdir") before being
// able to read files out of it. See GoogleCloudPlatform/gcsfuse#7.
func (s *SFTPSuite) TestWriteGCSReadSFTP(c *gc.C) {
	// Write our fixture to GCS.
	readFixture := uuid.NewV4().String()
	filename := "pippio-client-vault/testuser/" + readFixture
	gcsFile, err := s.cfs.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	c.Assert(err, gc.IsNil)

	// Use the filename as trivial content.
	var n int
	n, err = gcsFile.Write([]byte(readFixture))
	c.Assert(err, gc.IsNil)
	c.Check(n, gc.Equals, len(readFixture))
	c.Check(gcsFile.Close(), gc.IsNil)

	defer func() {
		err = s.cfs.Remove(filename)
		c.Assert(err, gc.IsNil)
	}()

	// Read the same file from SFTP and compare.
	sftpClient := s.connectSFTP(*endpoints.APIUsername, *endpoints.APIPassword, c)

	var sftpFile *sftp.File
	sftpFile, err = sftpClient.OpenFile(readFixture, os.O_RDONLY)
	c.Assert(err, gc.IsNil)

	readFixtureSftp := make([]byte, len(readFixture))
	n, err = sftpFile.Read(readFixtureSftp)
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, len(readFixture))
	c.Check(string(readFixtureSftp), gc.Equals, readFixture)
}

// Verifies that a fixture file staged via SFTP can be read using |cfs|.
func (s *SFTPSuite) TestWriteSFTPReadGCS(c *gc.C) {
	// Write our fixture to SFTP.
	readFixture := uuid.NewV4().String()
	sftpClient := s.connectSFTP(*endpoints.APIUsername, *endpoints.APIPassword, c)
	sftpFile, err := sftpClient.OpenFile(readFixture, os.O_WRONLY|os.O_CREATE)
	c.Assert(err, gc.IsNil)

	// Use the filename as trivial content.
	var n int
	n, err = sftpFile.Write([]byte(readFixture))
	c.Assert(err, gc.IsNil)
	c.Check(n, gc.Equals, len(readFixture))
	c.Check(sftpFile.Close(), gc.IsNil)

	defer func() {
		err = sftpClient.Remove(readFixture)
		c.Assert(err, gc.IsNil)
	}()

	// Read the same file from GCS and compare.
	filename := "pippio-client-vault/testuser/" + readFixture
	gcsFile, err := s.cfs.OpenFile(filename, os.O_RDONLY, 0)
	c.Assert(err, gc.IsNil)

	readFixtureGCS := make([]byte, len(readFixture))
	n, err = gcsFile.Read(readFixtureGCS)
	c.Check(err, gc.Equals, io.EOF)
	c.Check(n, gc.Equals, len(readFixture))
	c.Check(string(readFixtureGCS), gc.Equals, readFixture)
}

func (s *SFTPSuite) connectSFTP(user, pass string, c *gc.C) *sftp.Client {
	// Verify that SSH is up by using a bad username and password. If the
	// connection fails to open for any other reason than bad credentials, we
	// assume that SSH isn't running at all and we shouldn't execute the test.
	config := &ssh.ClientConfig{
		User: "baduser",
		Auth: []ssh.AuthMethod{ssh.Password("badpass")},
	}

	sshClient, err := ssh.Dial("tcp", *endpoints.SftpEndpoint, config)
	c.Assert(err, gc.NotNil)

	if !strings.HasPrefix(err.Error(), "ssh: handshake failed: ssh: unable to authenticate") {
		c.Log("Unexpected SSH error: " + err.Error())
		c.Skip("SSH server is not responding. Cannot run SFTP test.")
		return nil
	}

	// Retry with the specified user and password now that we know SSH is up.
	// Use a random algorithm to ensure all host keys are as expected over time.
	config.User = user
	config.Auth[0] = ssh.Password(pass)
	config.HostKeyCallback = s.hostKeyCallback
	i := rand.Intn(len(hostKeyAlgorithms))
	config.HostKeyAlgorithms = hostKeyAlgorithms[i : i+1]

	sshClient, err = ssh.Dial("tcp", *endpoints.SftpEndpoint, config)
	c.Assert(err, gc.IsNil)

	var sftpClient *sftp.Client
	sftpClient, err = sftp.NewClient(sshClient)
	c.Assert(err, gc.IsNil)

	return sftpClient
}

func (s *SFTPSuite) hostKeyCallback(hostname string, remote net.Addr, key ssh.PublicKey) error {
	var hostKey ssh.Signer
	switch key.Type() {
	case "ssh-dss":
		hostKey = s.keyDSA
	case "ssh-rsa":
		hostKey = s.keyRSA
	case "ecdsa-sha2-nistp256":
		hostKey = s.keyECDSA
	default:
		return errors.New("unexpected host key type: " + key.Type())
	}

	if key.Type() != hostKey.PublicKey().Type() {
		return fmt.Errorf("%s cipher mismatch: got %s expect %s",
			remote, key.Type(), hostKey.PublicKey().Type())
	}
	if string(key.Marshal()) != string(hostKey.PublicKey().Marshal()) {
		md5Actual := md5.New()
		md5Expect := md5.New()
		md5Actual.Write(key.Marshal())
		md5Expect.Write(hostKey.PublicKey().Marshal())
		return fmt.Errorf("%s key mismatch: got %s md5 %x, expect %s md5 %x",
			remote, key.Type(), md5Actual.Sum(nil),
			hostKey.PublicKey().Type(), md5Expect.Sum(nil))
	}
	return nil
}

var _ = gc.Suite(&SFTPSuite{})
