package cloudstore

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pippio/endpoints"
)

// File extends the read-only http.File interface with an io.Writer.
type File interface {
	http.File
	io.Writer

	// ContentSignature is a representation of the file's data, ideally
	// a content sum or ETag (in the case of cloud storage providers).
	// Calling this should not require a calculation that reads the whole file.
	ContentSignature() (string, error)
}

// FileSystem extends the read-only methods of http.FileSystem with methods
// capable of writing files, making directories, removing files or directories,
// and for producing "signed" URLs granting the URL bearer time-limited access
// rights to the file. Throughout the interface, returned errors are mapped into
// os-pkg errors testable with os.IsNotExist(), os.IsExist(), etc.
type FileSystem interface {
	http.FileSystem

	// Releases the FileSystem and associated resources.
	Close() error

	// Writes |to| by directly copying from |from|. Iff an error is encountered
	// (either via |to.Write()| or *|from.Read()|*), the partially-written content
	// is removed or never made observable on the target FileSystem (depending on
	// provider semantics). Otherwise, |to| is visible on the FileSystem after
	// the call completes. In all cases, |from| is invalidated (eg, Close()d)
	// after this call. Re-tryable bulk transfers should generally use
	// this method for all-or-nothing behavior.
	CopyAtomic(to File, from io.Reader) (n int64, err error)

	// Creates a directory |path|, along with any necessary parents.
	MkdirAll(name string, perm os.FileMode) error

	// Generalized open call. It opens the named file with the specified |flag|
	// and |perm|. For cloud file systems, |flag| and |perm| are interpreted and
	// mapped into the capabilities of the file system, and may be ignored.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	// Indicates whether |ToURL| produces a authorized URL.
	ProducesAuthorizedURL() bool

	// Removes the named file or directory.
	Remove(name string) error

	// Produces a URL which fully identifies the resource. Depending on the
	// provider, the URL should implicitly authorize the bearer for operation
	// |method| within |validFor| duration.
	ToURL(name, method string, validFor time.Duration) (*url.URL, error)

	// Similar to |filepath.Walk|, calls a |filepath.WalkFunc| for every
	// file or directory under the given |prefix|. Note that not all filesystems
	// surface the concept of directories, and minimally, each driver only
	// guarantees to return a recursive listing of files.
	Walk(root string, walkFn filepath.WalkFunc) error
}

// Selects a FileSystem implementation from |rawURL|. Implementations are
// determined by URL scheme, and the path roots the resulting FileSystem.
// Depending on provider, options are passed as URL query arguments.
func NewFileSystem(properties Properties, rawURL string) (FileSystem, error) {
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	if url.Scheme == "file" {
		return localFs{http.Dir(url.Path), false}, nil
	} else if url.Scheme == "gs" {
		var _, compress = url.Query()["compress"]
		return newGCSFS(properties, url.Host+url.Path, compress)
	} else if url.Scheme == "s3" {
		var sp Properties
		if properties == nil {
			sp, err = getS3ConfigProperties()
			if err != nil {
				return nil, err
			}
		} else {
			sp = properties
		}
		return newS3FS(sp, url.Host+url.Path)
	} else if url.Scheme == "sftp" {
		return newSFTPFs(properties, url.Host, url.Path, url.User)
	} else {
		return nil, errors.New("filesystem not supported: " + rawURL)
	}
}

func getS3ConfigProperties() (S3Properties, error) {
	endpoints.ParseFromEnvironment()
	var ak = *endpoints.AWSAccessKeyId
	var as = *endpoints.AWSSecretAccessKey

	return S3Properties{
		AWSAccessKeyID:     ak,
		AWSSecretAccessKey: as,
		S3Region:           "us-east-1",
		S3GlobalCannedACL:  s3.ObjectCannedACLBucketOwnerFullControl,
		S3SSEAlgorithm:     "",
	}, nil
}

func DefaultFileSystem(properties Properties) (FileSystem, error) {
	return NewFileSystem(properties, *endpoints.CloudFS)
}

// Returns a FileSystem backed by a new temporary directory. The returned
// FileSystem should be Close()d after use to clear temporary files.
func NewTmpFileSystem() FileSystem {
	path, err := ioutil.TempDir("", "tmp-cloud-fs")
	if err != nil {
		log.WithFields(log.Fields{"err": err, "path": path}).
			Panic("couldn't create tmp-fs")
	}
	return localFs{http.Dir(path), true}
}

// In a full cloud storage URL, the host part (initial component) of the URL is
// used as the bucket. That part may be specified by the initial cloud storage
// URL that the FileSystem was initialized with. Conversely, that URL could be
// blank except for the protocol, and client code may pass in the bucket name
// on every request.
//
// As both forms are valid, here we flatten any prefix and additional
// path component into one absolute path, then take the first component and
// remaining components as |bucket| and |subpath|.
//
// Respects any trailing slashes that may result from joining the prefix and
// the name together.
//
// This implementation is shared by GCS and S3. Examples:
//
// gs:// + foo/bar/baz = (foo, bar/baz)
// gs://bucket + foo/bar = (bucket, foo/bar)
// gs://bucket/foo + bar = (bucket, foo/bar)
func pathToBucketAndSubpath(prefix, name string) (bucket, subpath string) {
	var joined = path.Join(prefix, name)
	var parts = strings.SplitN(joined, "/", 2)
	var suffix string

	// Determine if the naive joining of |prefix| and |name| would result in a
	// trailing "/". This indicates that the user wants to open the file as a
	// directory.
	if (name == "" && strings.HasSuffix(prefix, "/")) || (name != "" && strings.HasSuffix(name, "/")) {
		suffix = "/"
	}

	if len(parts) == 2 && parts[1] != "" {
		// A non-bucket component is present. Respect the suffix.
		return parts[0], parts[1] + suffix
	} else {
		// Only a bucket component is present. Present no suffix to enable a
		// root directory perusal.
		return parts[0], ""
	}
}

// By convention, the user accessing a bucket URI wants to treat the final
// portion of the URI as an object prefix if the URI ends with a trailing
// slash, and not otherwise.
//
// This implementation is shared by GCS and S3.
func isBucketStoreDir(path string) bool {
	return path == "" || path[len(path)-1] == '/'
}
