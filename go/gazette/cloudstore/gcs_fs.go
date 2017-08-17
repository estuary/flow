package cloudstore

import (
	"context"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/Sirupsen/logrus"
	gzip "github.com/youtube/vitess/go/cgzip"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	GCPServiceAccount = flag.String("gcpServiceAccount",
		"/var/run/secrets/google-service-account/key.json",
		"Location to Google Service Account JSON file")
)

// Maps Google Cloud Storage into an API compatible with cloudstore.FileSystem.
type gcsFs struct {
	// Prefix roots all files within this filesystem.
	prefix string
	// Iff |compress|, files are transparently written with Content-Encoding: gzip.
	compress bool
	// Authenticated storage client.
	client *storage.Client
	// Options required for generating signed URLs.
	signOptions storage.SignedURLOptions
}

// Returns a gcsFs from the specified |credentials| in JSON format,
// for the given bucket and path |prefix|, optionally using gzip compression.
func newGCSFS(properties Properties, prefix string, compress bool) (*gcsFs, error) {
	var credentials, err = ioutil.ReadFile(*GCPServiceAccount)
	if err != nil {
		return nil, err
	}

	conf, err := google.JWTConfigFromJSON(credentials, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	}

	var ctx = context.Background()
	var client *storage.Client

	client, err = storage.NewClient(ctx, option.WithTokenSource(conf.TokenSource(ctx)))
	if err != nil {
		return nil, err
	}

	return &gcsFs{
		prefix:   prefix,
		compress: compress,
		client:   client,
		signOptions: storage.SignedURLOptions{
			GoogleAccessID: conf.Email,
			PrivateKey:     conf.PrivateKey,
		},
	}, nil
}

// Opens a GCS file for reading or for writing (O_RDWR is not supported).
// O_CREATE is enforced, and O_CREATE|O_EXCL is emulated (best effort) by
// checking for file existence prior to open. Files opened for O_RDONLY are
// not actually opened for reading by this call (only attributes are fetched).
// Instead, reader opens happen lazily on the first Read call.
func (fs *gcsFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	// TODO(johnny): |perm| is currently ignored. Should these be mapped
	// into owner / group / everyone ACL's?
	var bucket, path = pathToBucketAndSubpath(fs.prefix, name)
	var isDir = isBucketStoreDir(path)

	var exists bool
	var attrs *storage.ObjectAttrs

	// Check the current status of |path| on cloud storage. First determine if
	// |path| is a regular file by checking Attrs. If not, determine if |path|
	// should be treated as a directory by quering for subordinate files.
	if !isDir {
		var err error
		// Perform a stat to determine the existence of a regular file |path|.
		attrs, err = fs.client.Bucket(bucket).Object(path).Attrs(context.Background())

		if err == storage.ErrObjectNotExist {
			exists = false
		} else if err != nil {
			return nil, err
		} else {
			exists = true
		}

		if !exists {
			// |path| doesn't have a '/' suffix, but could still be a directory.
			// Check by querying with a suffix, and seeing if any files are returned.
			var iter = fs.client.Bucket(bucket).Objects(context.Background(),
				&storage.Query{Prefix: path + "/"})
			iter.PageInfo().MaxSize = 1

			if _, err := iter.Next(); err == iterator.Done {
				// Pass (not a directory).
			} else if err != nil {
				return nil, err
			} else {
				// Rewrite |path| as a directory.
				path = path + "/"
				isDir = true
			}
		}
	}

	// Is this a directory? Opens of directories for reading always succeed.
	// Otherwise fail.
	if isDir {
		if flag != os.O_RDONLY {
			return nil, errors.New("unsupported directory flags")
		}
		return &gcsFile{
			fs:     fs,
			bucket: bucket,
			path:   path,
			// Build an iterator used for successive Readdir calls.
			iter: fs.makeIterator(bucket, path, "/"),
		}, nil
	}

	// Walk through each supported flag combination, and emulate flag behaviors
	// by testing against the stat'd status of the file on cloud storage.
	if flag == os.O_RDONLY && !exists {
		return nil, os.ErrNotExist // Read which doesn't exist. Map to os error.
	} else if flag == os.O_RDONLY {
		// Read which exists. Return a file which will lazily open a reader.
		return &gcsFile{
			fs:         fs,
			bucket:     bucket,
			path:       path,
			attrs:      attrs,
			lazyReader: true,
		}, nil
	} else if flag == os.O_WRONLY|os.O_TRUNC && !exists {
		return nil, os.ErrNotExist // Write which doesn't exist. Map to os error.
	} else if flag == os.O_WRONLY|os.O_CREATE|os.O_EXCL && exists {
		return nil, os.ErrExist // Exclusive create which exists. Map to os error.
	} else if flag == os.O_WRONLY|os.O_TRUNC ||
		flag == os.O_WRONLY|os.O_CREATE|os.O_TRUNC ||
		flag == os.O_WRONLY|os.O_CREATE|os.O_EXCL {

		// Open for writing, potentially wrapped with a compressor.
		var w = fs.client.Bucket(bucket).Object(path).NewWriter(context.Background())
		var compressor io.WriteCloser

		// TODO(johnny, PUB-4052): Hack to skip gzip compression on recovery logs.
		// Fix this by implementing configurable compression on journal hierarchies.
		if fs.compress && !isRecoveryLog(name) {
			w.ContentEncoding = "gzip"
			w.ContentType = "application/octet-stream"
			compressor = gzip.NewWriter(w)
		}

		return &gcsFile{
			fs:         fs,
			bucket:     bucket,
			path:       path,
			attrs:      attrs,
			writer:     w,
			compressor: compressor,
		}, nil
	} else {
		return nil, errors.New("unsupported file flags")
	}
}

func (fs *gcsFs) Open(name string) (http.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *gcsFs) MkdirAll(path string, perm os.FileMode) error {
	// Directories implicitly exist in GCS.
	return nil
}

func (fs *gcsFs) Remove(name string) error {
	var bucket, path = pathToBucketAndSubpath(fs.prefix, name)
	var err = fs.client.Bucket(bucket).Object(path).Delete(context.Background())
	if err == storage.ErrObjectNotExist {
		return os.ErrNotExist // Map to os error.
	}
	if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == http.StatusNotFound {
		// TODO(johnny): Workaround (storage.DeleteObject() doesn't map this error).
		return os.ErrNotExist
	}
	return err
}

// CopyAtomic copies files contents, while being sensitive to the consistency
// guarantees provided by Google Cloud Storage. In particular, |to| is aborted
// (gcsFile.rawWriter.Close() is not called) if a write *or read* error occurs.
func (fs *gcsFs) CopyAtomic(to File, from io.Reader) (int64, error) {
	return to.(*gcsFile).transfer(from)
}

func (fs *gcsFs) ToURL(path, method string, validFor time.Duration) (*url.URL, error) {
	var bucket, subPath = pathToBucketAndSubpath(fs.prefix, path)

	var options = fs.signOptions // Copy common options.
	options.Method = method
	options.Expires = time.Now().Add(validFor)

	var rawURL, err = storage.SignedURL(bucket, subPath, &options)
	if err != nil {
		return nil, err
	}
	return url.Parse(rawURL)
}

func (fs *gcsFs) ProducesAuthorizedURL() bool {
	return true
}

func (fs *gcsFs) Close() error {
	return nil // No-op.
}

// Emits all files underneath |root| to |walkFn|.
//
// Takes advantage of the fact that there are no real directories in GCS, only
// prefixes. Initiates an object listing underneath the root (a prefix) but
// without any Delimiter specified. As a result, all files under the prefix,
// regardless of directory-depth, are returned in a small number of API calls
// (typically just one, but subject to pagination if the number of files is
// large.)
//
// As this scan doesn't know about even virtual directory boundaries, the
// |filepath.SkipDir| API allowing walk-functions to skip an entire directory
// isn't implemented and should not be used.
func (fs *gcsFs) Walk(root string, walkFn filepath.WalkFunc) error {
	var bucket, path = pathToBucketAndSubpath(fs.prefix, root)
	// Use no delimiter to return all files matching the initial bucket and prefix.
	var iter = fs.makeIterator(bucket, path, "")
	var obj, err = iter.Next()

	for ; err == nil; obj, err = iter.Next() {
		// Sometimes there are invisible files named after the prefix,
		// these aren't real files so pretend they don't exist.
		if strings.HasSuffix(obj.Name, "/") {
			continue
		}

		var fp = &gcsFile{
			fs:         fs,
			bucket:     bucket,
			path:       obj.Name,
			attrs:      obj,
			generation: obj.Generation,
		}

		// Strip the full prefix. |rel| is now relative to |path|.
		var rel, rerr = filepath.Rel(path, obj.Name)
		if rerr != nil {
			return rerr
		}

		if werr := walkFn(filepath.Join(root, rel), fp, nil); werr == filepath.SkipDir {
			return errors.New("SkipDir not implemented for gcsFs")
		} else if werr != nil {
			// Allow caller to abort Walk operation.
			return werr
		}
	}

	if err == iterator.Done {
		return nil
	}
	return err
}

func (fs *gcsFs) prefixed(name string) (bucket, subpath string) {
	var joined = filepath.Join(fs.prefix, name)
	var parts = strings.SplitN(joined, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	} else {
		return parts[0], ""
	}
}

func (fs *gcsFs) makeIterator(bucket, path, delimiter string) *storage.ObjectIterator {
	return fs.client.Bucket(bucket).Objects(context.Background(),
		&storage.Query{
			Prefix:    path,
			Delimiter: delimiter,
		})
}

type gcsFile struct {
	fs           *gcsFs
	bucket, path string

	// Attrs result from file open.
	attrs *storage.ObjectAttrs

	// Cursor from last Readdir.
	iter *storage.ObjectIterator

	lazyReader bool
	reader     io.ReadCloser
	readOffset int64

	// If |compressor| is non-nil, it is backed by |writer|.
	writer     *storage.Writer
	compressor io.WriteCloser

	// Generation info
	generation int64

	err error
}

// File interface method.
func (f *gcsFile) Read(p []byte) (int, error) {
	if f.lazyReader {
		// Lazy open.
		f.reader, f.err = f.fs.client.Bucket(f.bucket).Object(f.path).NewReader(context.Background())
		f.lazyReader = false
	}
	if f.err != nil {
		return 0, f.err
	} else if f.reader == nil {
		return 0, errors.New("not a gcsFile reader")
	}

	var n int
	n, f.err = f.reader.Read(p)
	f.readOffset += int64(n)
	return n, f.err
}

// File interface method.
func (f *gcsFile) Write(p []byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	} else if f.writer == nil {
		return 0, errors.New("not a gcsFile writer")
	}

	var n int
	if f.compressor != nil {
		n, f.err = f.compressor.Write(p)
	} else {
		n, f.err = f.writer.Write(p)
	}

	if f.err != nil {
		f.writer.CloseWithError(f.err)
		if f.compressor != nil {
			f.compressor.Close()
		}
		f.writer, f.compressor = nil, nil
	}
	return n, f.err
}

// File interface method.
func (f *gcsFile) Close() error {
	if f.reader != nil {
		f.err = f.reader.Close()
		f.reader = nil
	} else if f.writer != nil {
		if f.compressor != nil {
			f.err = f.compressor.Close()
		}
		if f.err == nil {
			f.err = f.writer.Close()
		} else {
			f.writer.CloseWithError(f.err)
		}
		f.compressor, f.writer = nil, nil
	}
	return f.err
}

// File interface method.
func (f *gcsFile) Seek(offset int64, whence int) (int64, error) {
	if f.writer != nil {
		return 0, errors.New("cannot seek a gcsFs writer")
	}
	// Make |offset| absolute.
	if whence == io.SeekStart {
	} else if whence == io.SeekCurrent {
		offset += f.readOffset
	} else if whence == io.SeekEnd {
		return 0, errors.New("a gcsFs reader cannot seek relative to file end")
	}
	// If seeking before the current |readOffset|, re-open |f.reader|.
	if offset < f.readOffset && (f.err == nil || f.err == io.EOF) {
		f.reader.Close()
		f.reader, f.err = f.fs.client.Bucket(f.bucket).Object(f.path).NewReader(context.Background())

		f.readOffset = 0
		f.err = nil
	} else if f.err != nil {
		return f.readOffset, f.err
	}
	// Errors and offsets are tracked by f.Read().
	io.CopyN(ioutil.Discard, f, offset-f.readOffset)
	return f.readOffset, f.err
}

// File interface method.
func (f *gcsFile) Readdir(count int) ([]os.FileInfo, error) {
	if f.iter == nil {
		return nil, errors.New("nil directory iterator")
	}

	var results []os.FileInfo
	var obj, err = f.iter.Next()

	for ; err == nil; obj, err = f.iter.Next() {
		if obj.Prefix != "" {
			// Synthetic subdirectory prefix "file".
			results = append(results, &gcsFile{
				fs:     f.fs,
				bucket: f.bucket,
				path:   obj.Prefix,
			})
		} else if obj.Name == f.path {
			// A directory can also have a regular file with the same path.
			// Skip it, should one occur.
		} else {
			// Regular file.
			results = append(results, &gcsFile{
				fs:     f.fs,
				bucket: f.bucket,
				path:   obj.Name,
				attrs:  obj,
			})
		}

		// Readdir defines count <= 0 to mean "return all entries".
		if count > 0 && len(results) == count {
			break
		}
	}

	// Convert iterator.Done to Readdir EOF semantics.
	if err == iterator.Done {
		if len(results) == 0 && count > 0 {
			err = io.EOF
		} else {
			err = nil
		}
	}
	return results, err
}

// File interface method
func (f *gcsFile) ContentSignature() (string, error) {
	if f.IsDir() {
		return "", os.ErrNotExist
	}
	return string(f.attrs.MD5), nil
}

// A gcsFile is also a os.FileInfo.
func (f *gcsFile) Stat() (os.FileInfo, error) {
	return f, nil
}

// os.FileInfo interface method.
func (f *gcsFile) Name() string {
	name := f.path

	if name == "" {
		return f.bucket
	} else if name[len(name)-1] == '/' {
		name = name[:len(name)-1]
	}
	return name[strings.LastIndex(name, "/")+1:]
}

// os.FileInfo interface method. May return size of the compressed file,
// and not the actual bytestream length.
func (f *gcsFile) Size() int64 {
	if f.attrs != nil {
		return f.attrs.Size
	}
	return 0
}

// os.FileInfo interface method.
func (f *gcsFile) Mode() os.FileMode {
	log.Panic("gcsFile does not implement Mode()")
	return 0
}

// os.FileInfo interface method.
func (f *gcsFile) ModTime() time.Time {
	if f.attrs != nil {
		return f.attrs.Updated
	}
	return time.Time{}
}

// os.FileInfo interface method.
func (f *gcsFile) IsDir() bool {
	return isBucketStoreDir(f.path)
}

// os.FileInfo interface method.
func (f *gcsFile) Sys() interface{} {
	return nil
}

func (f *gcsFile) Version() int64 {
	return f.generation
}

func (f *gcsFile) transfer(from io.Reader) (int64, error) {
	var n int64

	if f.compressor != nil {
		n, f.err = io.Copy(f.compressor, from)
	} else {
		n, f.err = io.Copy(f.writer, from)
	}

	if f.err != nil {
		// The transfer failed, CloseWithError to abort it. GCS features atomic
		// upload, so only a successful file is ever made visible.
		f.writer.CloseWithError(f.err)
		if f.compressor != nil {
			f.compressor.Close()
		}
		f.writer, f.compressor = nil, nil
		return n, f.err
	}

	return n, f.Close()
}

func isRecoveryLog(path string) bool {
	return strings.Contains(path, "recovery-logs")
}
