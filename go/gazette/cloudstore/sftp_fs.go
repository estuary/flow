package cloudstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/samuel/go-socks/socks"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

// Used for properties.Get.
const (
	SFTPUsername = "SFTPUsername"
	SFTPPassword = "SFTPPassword"
	SFTPPort     = "SFTPPort"
	SFTPReqProxy = "SFTPReqProxy"
	SFTPKey      = "SFTPKey"

	SFTPDefaultPort = "22"
)

const (
	// All current versions of the IETF spec mandate that servers should accept
	// packets of at least 32k, but can reject larger packets. To prevent additional
	// network overhead from trying to "find" the server's acceptable packet
	// length, we'll just assume that there's a hard limit at 32k, and do our best
	// to write in chunks as close to that as possible.
	maxSFTPPacketSize = 1 << 15

	// Turn on/off in-memory buffering for writes
	useWriteBuffer = true
)

// Redeclaring SSH error codes since the originals are not exported
// http://api.libssh.org/master/group__libssh__sftp.html#member-group
// NOTE(Azim): File exists errors are actually mapping to SSH_ERR_FAILURE
// (4) instead of file exists (11). Compensating for that since we need to
// minimally be able to tell when a file exists, although this may mask others.
const (
	SSHErrFileNotFound = 2
	SSHErrFileExists   = 4
)

// Luckily sftp.File already meets most of the File interface.
type sftpFile struct {
	*sftp.File

	// Lazy-initialized buffer to spool file writes in memory.
	buf *bufio.Writer

	// Need this so we can Remove the file if CopyAtomic fails.
	partialPath string
}

// Write is a buffered write call to the underlying SFTP file. We use the SFTP
// max packet size as our buffer size to allow Write to be called many times
// with small amounts of data and yet only incur the network overhead of SFTP
// once per 32k chunk, optimizing chatty writers over slow connections.
func (f *sftpFile) Write(p []byte) (int, error) {
	if useWriteBuffer && f.buf == nil {
		f.buf = bufio.NewWriterSize(f.File, maxSFTPPacketSize)
	}
	if useWriteBuffer {
		return f.buf.Write(p)
	}
	return f.File.Write(p)
}

// Close flushes the buffered writes and closes the remote connection. It's
// important for the caller to check the error here, since the final buffered
// write may not have been sent until this point.
func (f *sftpFile) Close() error {
	if f.buf == nil {
		// Fall through
	} else if err := f.buf.Flush(); err != nil {
		return fmt.Errorf("flushing buffer: %s", err.Error())
	} else if err := f.File.Close(); err != nil {
		return fmt.Errorf("closing file: %s", err.Error())
	}
	return nil
}

func (f *sftpFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, fmt.Errorf("Called Readdir on sftp file")
}

// For SFTP filesystems, ContentSignature is the filesize
//TODO(Azim): Re-open the file and hash the first ~1MiB or so
func (f *sftpFile) ContentSignature() (string, error) {
	var stat, err = f.Stat()
	if err != nil {
		return "", err
	} else {
		return strconv.FormatInt(stat.Size(), 10), nil
	}
}

type sftpDir struct {
	path       string
	client     *sftp.Client
	readdirObj *readdir
}

func (d *sftpDir) Readdir(count int) ([]os.FileInfo, error) {
	if d.readdirObj == nil {
		d.readdirObj = &readdir{client: d.client, path: d.path}
	}
	return d.readdirObj.Read(count)
}

func (d *sftpDir) Stat() (os.FileInfo, error) {
	return d.client.Lstat(d.path)
}

// No-op instead of an error. This is what os.File does.
func (d *sftpDir) Close() error {
	return nil
}

// No-op, since sftpDir needs to implement File
func (d *sftpDir) ContentSignature() (string, error) {
	return "", errors.New("no directory content signature")
}

func (d *sftpDir) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("Read call on SFTP Dir %v", d.path)
}

func (d *sftpDir) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("Seek call on SFTP Dir %v", d.path)
}

func (d *sftpDir) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("Write call on SFTP Dir %v", d.path)
}

// Object to encapsulate the readdir behavior
type readdir struct {
	storedFiles []os.FileInfo
	depleted    bool

	client *sftp.Client
	path   string
}

// Always load remote into stored files then read from stored.
func (d *readdir) Read(count int) ([]os.FileInfo, error) {
	// Do a remote Readdir and store results if none stored
	if d.storedFiles == nil {
		if results, err := d.Raw(); err != nil {
			return nil, err
		} else {
			d.storedFiles = results
		}
	}

	return d.Cached(count)
}

func (d *readdir) Raw() ([]os.FileInfo, error) {
	return d.client.ReadDir(d.path)
}

func (d *readdir) Cached(count int) ([]os.FileInfo, error) {
	if d.depleted {
		return nil, io.EOF
	}

	if count == -1 || len(d.storedFiles) <= count {
		var res = d.storedFiles
		d.storedFiles = nil
		d.depleted = true
		return res, nil
	}

	var res = d.storedFiles[:count]
	d.storedFiles = d.storedFiles[count:]
	return res, nil
}

// Cloudstore compliant API around SFTP.
type sftpFs struct {
	properties  Properties
	host, path  string
	userFromURL *url.Userinfo
	client      *sftp.Client
}

func newSFTPFs(properties Properties, host string, path string, user *url.Userinfo) (*sftpFs, error) {
	// All paths are relative to the ftp root. If an absolute path was passed in,
	// strip out the leading / to make it relative.
	if path[0] == '/' {
		path = path[1:]
	}
	if path == "" {
		path = "."
	}

	var err error
	var res = sftpFs{properties, host, path, user, nil}
	res.client, err = res.makeSFTPClient()
	if err != nil {
		return nil, err
	}
	log.WithField("fs", res).Debug("using sftp client")
	return &res, nil
}

// Generalized open call. It opens the named file with the specified |flag|
// and |perm|. Returns a file object or a dir object, both of which
// meet the File interface.
func (s *sftpFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	log.WithFields(log.Fields{
		"name": name,
		"path": s.client.Join(s.path, name),
		"flag": flag,
		"perm": perm,
	}).Debug("OpenFile called")
	var path = s.client.Join(s.path, name)

	if isDir, err := s.isDir(path); err != nil {
		return nil, err
	} else if isDir {
		return &sftpDir{path: path, client: s.client}, nil
	}

	if file, err := s.client.OpenFile(path, flag); err != nil {
		// Convert SSH errors into OS-Compliant errors
		if isSSHError(err, SSHErrFileNotFound) {
			return nil, os.ErrNotExist
		} else if isSSHError(err, SSHErrFileExists) {
			// This branch is for the EXCL flag.
			// Not sure if this is catching it properly
			return nil, os.ErrExist
		}
		return nil, err

	} else {
		return &sftpFile{File: file, partialPath: name}, err
	}

}

// Broke this out from MkdirAll cause it reads a lot clearer this way.
// It splits a/b/c into a,a/b,a/b/c
func accumPaths(fullPath, startPath string, join func(...string) string) []string {
	var parts = strings.Split(join(startPath, fullPath), "/")
	var res = make([]string, len(parts))
	var accumPath string

	for i, part := range parts {
		if accumPath != "" {
			accumPath = join(accumPath, part)
		} else {
			accumPath = part
		}
		res[i] = accumPath
	}
	return res
}

// Creates a directory |path|, along with any necessary parents.
func (s *sftpFs) MkdirAll(fullPath string, perm os.FileMode) error {
	fullPath = filepath.Clean(fullPath)
	for _, nextPath := range accumPaths(fullPath, s.path, s.client.Join) {
		if isDir, err := s.isDir(nextPath); err != nil {
			return err
		} else if !isDir && nextPath != "" {
			if err := s.client.Mkdir(nextPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *sftpFs) Remove(name string) error {
	var path = s.client.Join(s.path, name)
	if err := s.client.Remove(path); err != nil {
		if isSSHError(err, SSHErrFileNotFound) {
			return os.ErrNotExist
		}
		return err
	}
	return nil
}

func (s *sftpFs) ToURL(name, method string, validFor time.Duration) (*url.URL, error) {
	return &url.URL{
		Scheme: "sftp",
		User:   url.UserPassword(s.username(), s.password()),
		Host:   s.host + ":" + s.port(),
		Path:   s.path,
	}, nil
}

func (s *sftpFs) ProducesAuthorizedURL() bool {
	return false
}

func (s *sftpFs) CopyAtomic(to File, from io.Reader) (n int64, err error) {
	if n, err = io.Copy(to, from); err != nil {
		s.Remove(to.(*sftpFile).partialPath)
	} else {
		to.Close()
	}
	return
}

// Releases the FileSystem and associated resources.
func (s *sftpFs) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *sftpFs) Open(name string) (http.File, error) {
	return s.OpenFile(name, 0, os.ModeDir)
}

func (s *sftpFs) Walk(root string, walkFn filepath.WalkFunc) error {
	var path = s.client.Join(s.path, root)

	// Recast paths so that they are relative to |s.path|.
	var walker = s.client.Walk(path)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			return err
		} else if rel, err := filepath.Rel(s.path, walker.Path()); err != nil {
			return err
		} else if rel == "." || rel == ".." {
			// Never return "." or ".." as they are not real directories.
		} else if walkFn(rel, walker.Stat(), err); err != nil {
			return err
		}
	}

	return nil
}

func (s *sftpFs) port() string {
	var res = s.properties.Get(SFTPPort)
	if res == "" {
		res = SFTPDefaultPort
	}
	return res
}

func (s *sftpFs) username() string {
	if s.userFromURL != nil && s.userFromURL.Username() != "" {
		return s.userFromURL.Username()
	}
	return s.properties.Get(SFTPUsername)
}

func (s *sftpFs) password() string {
	if s.userFromURL == nil {
		return s.properties.Get(SFTPPassword)
	} else if pass, found := s.userFromURL.Password(); found {
		return pass
	}
	return s.properties.Get(SFTPPassword)
}

func (s *sftpFs) useKeyAuth() bool {
	return s.properties.Get(SFTPKey) != ""
}

func (s *sftpFs) requiresProxy() bool {
	var prox = s.properties.Get(SFTPReqProxy)
	var b bool
	var err error
	if b, err = strconv.ParseBool(prox); err != nil {
		// If we have a problem parsing the req_proxy boolean, log it as a
		// warning, but continue.
		log.WithFields(log.Fields{"require_proxy": prox, "err": err}).
			Warn("couldn't parse proxy bool")
		return false
	}
	return b
}

func (s *sftpFs) makeSFTPClient() (*sftp.Client, error) {
	var auth ssh.AuthMethod
	if s.useKeyAuth() {
		var keyBytes = []byte(s.properties.Get(SFTPKey))

		if signer, err := ssh.ParsePrivateKey(keyBytes); err != nil {
			return nil, err
		} else {
			auth = ssh.PublicKeys(signer)
		}
	} else {
		auth = ssh.Password(s.password())
	}

	var config = &ssh.ClientConfig{
		User: s.username(),
		Auth: []ssh.AuthMethod{auth},
	}

	if sshClient, err := makeSSHClient(s.host+":"+s.port(), config, s.requiresProxy()); err != nil {
		return nil, err
	} else if sftpClient, err := sftp.NewClient(sshClient); err != nil {
		return nil, err
	} else {
		return sftpClient, err
	}
}

// makeSSHClient creates an SSH Client, forwarding the connection through the
// SOCKS jumphost if required.
func makeSSHClient(addr string, config *ssh.ClientConfig, reqProxy bool) (*ssh.Client, error) {
	var baseConnection net.Conn
	var err error

	if reqProxy {
		var proxy = &socks.Proxy{socksEndpoint(), "", ""}
		baseConnection, err = proxy.Dial("tcp", addr)
	} else {
		baseConnection, err = net.Dial("tcp", addr)
	}

	if err != nil {
		return nil, err
	}

	// Vars which we take from the NewClientConn and pass straight into NewClient:
	var (
		conn  ssh.Conn
		newCh <-chan ssh.NewChannel
		reqCh <-chan *ssh.Request
	)

	conn, newCh, reqCh, err = ssh.NewClientConn(baseConnection, addr, config)
	if err != nil {
		return nil, err
	}
	return ssh.NewClient(conn, newCh, reqCh), nil
}

func socksEndpoint() string {
	var socksHost = os.Getenv("SOCKS_SERVER_SERVICE_HOST")
	var socksPort = os.Getenv("SOCKS_SERVER_SERVICE_PORT")
	if socksHost == "" || socksPort == "" {
		return "127.0.0.1:1080"
	} else {
		return socksHost + ":" + socksPort
	}
}

func isSSHError(err error, sshCode uint32) bool {
	if sftpErr, ok := err.(*sftp.StatusError); ok {
		return sftpErr.Code == sshCode
	}
	return false
}

// There is no direct way to check is a file/dir exists other than calling Lstat
// and checking if the error is a file not found error.
// If we get any other error, return it.
// Eat any file not found errors and return false
func (s *sftpFs) isDir(path string) (bool, error) {
	if stat, err := s.client.Lstat(path); err == nil {
		return stat.IsDir(), nil
	} else if err == os.ErrNotExist || isSSHError(err, SSHErrFileNotFound) {
		return false, nil
	} else {
		return false, err
	}
}
