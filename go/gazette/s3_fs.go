package cloudstore

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/pippio/keepalive"
)

const (
	AWSAccessKeyID     = "AWSAccessKeyID"
	AWSSecretAccessKey = "AWSSecretAccessKey"
	S3Region           = "S3Region"
	S3GlobalCannedACL  = "S3GlobalCannedACL"
	S3SSEAlgorithm     = "S3SSEAlgorithm"
)

// Maps Amazon S3 into an API compatible with cloudstore.FileSystem.
type s3Fs struct {
	properties Properties
	// Prefix roots all files within this filesystem.
	prefix string
}

// Opens a S3 file for reading or for writing (O_RDWR is not supported).
// O_CREATE is enforced, and O_CREATE|O_EXCL is emulated (best effort) by
// checking for file existence prior to open. Files opened for O_RDONLY are
// not actually opened for reading by this call (they're only stat'd): rather,
// read opens happen lazily, on the first Read() call.
func (fs *s3Fs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	// TODO(johnny): |perm| is currently ignored. Should these be mapped
	// into owner / group / everyone ACL's?
	bucket, path := pathToBucketAndSubpath(fs.prefix, name)
	var svc = fs.svc()
	var isDir = isBucketStoreDir(path)
	var statObject *s3.Object
	var exists bool

	// Check the current status of |path| on cloud storage. First determine if
	// |path| is a regular file via StatObject(). If not, determine if |path|
	// should be treated as a directory by quering for subordinate files.
	if !isDir {
		var headParams = s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		}

		resp, err := svc.HeadObject(&headParams)
		if isAWSNotFound(err) {
			// File does not exist, not a fatal error.
		} else if err != nil {
			// Unrelated error, treat as fatal.
			return nil, err
		} else {
			// File exists; make a *s3.Object from the HeadObject result.
			statObject = &s3.Object{
				ETag:         resp.ETag,
				Key:          aws.String(path),
				LastModified: resp.LastModified,
				// Owner field not set by this implementation.
				Size:         resp.ContentLength,
				StorageClass: resp.StorageClass,
			}
			exists = true
		}

		if !exists {
			// |path| doesn't have a '/' suffix, but could still be a directory.
			// Check by querying with a suffix, and seeing if any files are returned.
			// TODO(joshk): This query could store the first N files in the prefix,
			// but instead we just repeat it later in Readdir().
			var listParams = s3.ListObjectsV2Input{
				Bucket:    aws.String(bucket),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int64(1),
				Prefix:    aws.String(path + "/"),
			}

			listObjects, err := svc.ListObjectsV2(&listParams)
			if err != nil {
				return nil, err
			} else if len(listObjects.Contents) != 0 || len(listObjects.CommonPrefixes) != 0 {
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
		return &s3File{
			svc:    svc,
			bucket: aws.String(bucket),
			key:    aws.String(path),
		}, nil
	}

	// Walk through each supported flag combination, and emulate flag behaviors
	// by testing against the stat'd status of the file on cloud storage.
	if flag == os.O_RDONLY && !exists {
		return nil, os.ErrNotExist // Read which doesn't exist. Map to os error.
	} else if flag == os.O_RDONLY {
		// Read which exists. Return a file which will lazily open a reader.
		return &s3File{
			svc:    svc,
			bucket: aws.String(bucket),
			object: statObject,
			key:    aws.String(path),
		}, nil
	} else if flag == os.O_WRONLY|os.O_TRUNC && !exists {
		return nil, os.ErrNotExist // Write which doesn't exist. Map to os error.
	} else if flag == os.O_WRONLY|os.O_CREATE|os.O_EXCL && exists {
		return nil, os.ErrExist // Exclusive create which exists. Map to os error.
	} else if flag == os.O_WRONLY|os.O_TRUNC ||
		flag == os.O_WRONLY|os.O_CREATE|os.O_TRUNC ||
		flag == os.O_WRONLY|os.O_CREATE|os.O_EXCL {

		// Canned ACL takes effect if explicitly non-blank. Otherwise, defaults
		// to ObjectCannedACLBucketOwnerFullControl.
		var cannedACL *string
		if specACL := fs.properties.Get(S3GlobalCannedACL); specACL != "" {
			cannedACL = aws.String(specACL)
		} else {
			cannedACL = aws.String(s3.ObjectCannedACLBucketOwnerFullControl)
		}

		// Begin a multipart upload.
		var params = s3.CreateMultipartUploadInput{
			ACL:                  cannedACL,
			Bucket:               aws.String(bucket),
			Key:                  aws.String(path),
			ServerSideEncryption: fs.sseAlgorithm(),
		}

		resp, err := svc.CreateMultipartUpload(&params)
		if err != nil {
			return nil, err
		} else if resp.UploadId == nil {
			return nil, errors.New("expected UploadId in MultipartUpload creation")
		}

		return &s3File{
			svc:      svc,
			bucket:   aws.String(bucket),
			key:      aws.String(path),
			uploadId: resp.UploadId,
		}, nil
	} else {
		return nil, errors.New("unsupported file flags")
	}
}

func (fs *s3Fs) Open(name string) (http.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *s3Fs) MkdirAll(path string, perm os.FileMode) error {
	// Directories implicitly exist in S3.
	return nil
}

func (fs *s3Fs) Remove(name string) error {
	bucket, path := pathToBucketAndSubpath(fs.prefix, name)
	var svc = fs.svc()

	// DeleteObject won't tell you if the file didn't exist. HeadObject first.
	var headParams = s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	}

	_, err := svc.HeadObject(&headParams)
	if isAWSNotFound(err) {
		return os.ErrNotExist // Map to os error.
	}

	// The file exists. Attempt to delete it.
	var deleteParams = s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	}

	_, err = svc.DeleteObject(&deleteParams)
	return err
}

// CopyAtomic copies files contents, while being sensitive to the consistency
// guarantees provided by Amazon S3. In particular, |to| is aborted
// (s3File.uploadId is aborted explicitly) if a write *or read* error occurs.
func (fs *s3Fs) CopyAtomic(to File, from io.Reader) (n int64, err error) {
	if n, err = io.Copy(to.(*s3File), from); err == nil {
		// Completes the multipart upload.
		to.Close()
	}
	return

}

// For the specified |path|, generates a signed URL which makes it so that the
// credential keypair is not required to access the path, for the amount of
// time specified by |validFor|.
func (fs *s3Fs) ToURL(path, method string, validFor time.Duration) (*url.URL, error) {
	var bucket, subPath = pathToBucketAndSubpath(fs.prefix, path)
	var params = s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(subPath),
	}

	req, _ := fs.svc().GetObjectRequest(&params)
	if urlStr, err := req.Presign(validFor); err != nil {
		return nil, err
	} else if urlObj, err := url.Parse(urlStr); err != nil {
		return nil, err
	} else {
		return urlObj, nil
	}
}

func (fs *s3Fs) ProducesAuthorizedURL() bool {
	return true
}

func (fs *s3Fs) Close() error {
	return nil // No-op.
}

// Emits all files underneath |root| to |walkFn|.
//
// Takes advantage of the fact that there are no real directories in S3, only
// prefixes. Initiates an object listing underneath the root (a prefix) but
// without any Delimiter specified. As a result, all files under the prefix,
// regardless of directory-depth, are returned in a small number of API calls
// (typically just one, but subject to continuation if the number of files is
// large.)
//
// As this scan doesn't know about even virtual directory boundaries, the
// |filepath.SkipDir| API allowing walk-functions to skip an entire directory
// isn't implemented and should not be used.
func (fs *s3Fs) Walk(root string, walkFn filepath.WalkFunc) error {
	var bucket, subPath = pathToBucketAndSubpath(fs.prefix, root)
	var svc = fs.svc()
	var continuation *string

	for {
		var listParams = s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuation,
			// No Delimiter set.
			MaxKeys: s3MaxListObjectsKeys,
			Prefix:  aws.String(subPath),
		}

		var objects, err = svc.ListObjectsV2(&listParams)
		if err != nil {
			return err
		}

		for i := range objects.Contents {
			var fp = &s3File{
				svc:    svc,
				bucket: aws.String(bucket),
				key:    objects.Contents[i].Key,
				object: objects.Contents[i],
			}

			// Strip the full prefix. |rel| is now relative to |path|.
			var rel, err = filepath.Rel(subPath, *objects.Contents[i].Key)
			if err != nil {
				return err
			}

			if werr := walkFn(filepath.Join(root, rel), fp, nil); werr == filepath.SkipDir {
				// The underlying API call no longer has a concept of directories,
				// so we'd have to work really hard to enable SkipDir. Until
				// there is such a use-case, punt.
				return errors.New("SkipDir not implemented for s3Fs")
			} else if werr != nil {
				// Allow caller to abort Walk operation.
				return werr
			}
		}

		if objects.IsTruncated != nil && !*objects.IsTruncated {
			return nil
		}

		continuation = objects.NextContinuationToken
	}

	return nil
}

// TODO(joshk): Support IAM roles. Only matters if we are actually deployed on AWS.
func (fs *s3Fs) credentials() *credentials.Credentials {
	return credentials.NewStaticCredentials(fs.properties.Get(AWSAccessKeyID),
		fs.properties.Get(AWSSecretAccessKey), "")
}

func (fs *s3Fs) region() string {
	return fs.properties.Get(S3Region)
}

func (fs *s3Fs) sseAlgorithm() *string {
	if a := fs.properties.Get(S3SSEAlgorithm); a != "" {
		return aws.String(a)
	} else {
		return nil
	}
}

func (fs *s3Fs) svc() *s3.S3 {
	// S3 files with Content-Encoding: gzip will get transparently decompressed
	// with the default http transport, a behavior that we have to manually disable.
	var client = &http.Client{
		Transport: &http.Transport{
			Dial:               keepalive.Dialer.Dial,
			DisableCompression: true,
		},
	}
	var config = aws.NewConfig().WithCredentials(fs.credentials()).
		WithRegion(fs.region()).WithHTTPClient(client)
	var session = session.New(config)

	return s3.New(session)
}

func isAWSNotFound(err error) bool {
	if awsErr, ok := err.(awserr.RequestFailure); ok {
		return awsErr.StatusCode() == http.StatusNotFound
	} else {
		return false
	}
}
