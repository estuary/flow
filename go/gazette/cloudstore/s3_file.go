package cloudstore

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// Buffer roughly this much data in memory before flushing a multipart
	// chunk. (The true maximum multipart upload fragment size is much larger.)
	// Logistically, 1GiB chunks are more efficient, but we'd probably want to
	// spool to disk instead. This maximum spool size makes our maximum single
	// object file size 1TiB, as 10000 parts are allowed to any S3 object.
	MaxSpoolSizeBytes = 1024 * 1024 * 100
)

var (
	// By default, the API gives us 1000 keys per ListObjects call when we
	// leave this nil. Test cases can set this to artifically force multiple
	// calls to ListObjects.
	s3MaxListObjectsKeys *int64
)

type s3File struct {
	svc         *s3.S3
	bucket, key *string
	object      *s3.Object

	// TODO(joshk): Factor members below into mini structs and use a union type
	// here. Issue #2039

	// Used in file read operations.
	readCloser io.ReadCloser
	readOffset int64

	// Used in directory read operations.
	results      []os.FileInfo
	resultsIndex int

	// Used in write operations.
	uploadId      *string
	uploadedParts []*s3.CompletedPart
	spool         bytes.Buffer

	// Global error state for this file.
	err error
}

// File interface method.
func (f *s3File) Read(p []byte) (int, error) {
	var reader io.ReadCloser

	reader, f.err = f.reader()
	if f.err != nil {
		return 0, f.err
	}

	var n int
	n, f.err = reader.Read(p)
	f.readOffset += int64(n)
	return n, f.err
}

// File interface method.
func (f *s3File) Write(p []byte) (int, error) {
	var buf = &f.spool

	// Write to in-memory spool.
	n, err := buf.Write(p)
	if err != nil {
		return n, err
	}

	// If necessary, spill spool to S3 multipart chunk.
	if buf.Len() > MaxSpoolSizeBytes {
		if f.err = f.uploadSpool(); f.err != nil {
			return 0, f.err
		}
	}
	return n, err
}

// File interface method.
func (f *s3File) Close() error {
	if f.IsDir() {
		// Closing a directory object is a no-op.
	} else if f.readCloser != nil {
		// Here, we don't use reader(), as that would cause an unopened
		// readCloser to be opened before immediately closing it.
		f.err = f.readCloser.Close()
		f.readCloser = nil
	} else if f.isUpload() {
		// Drain final multipart chunk, if necessary. If uploading a totally
		// blank file, we still need to provide at least one part.
		if f.spool.Len() > 0 || len(f.uploadedParts) == 0 {
			if f.err = f.uploadSpool(); f.err != nil {
				return f.err
			}
		}

		var params = s3.CompleteMultipartUploadInput{
			Bucket:   f.bucket,
			Key:      f.key,
			UploadId: f.uploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: f.uploadedParts,
			},
		}

		_, f.err = f.svc.CompleteMultipartUpload(&params)
	} else {
		panic("cannot determine kind of s3File object for Close()")
	}
	return f.err
}

// File interface method.
func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	if f.isUpload() {
		return 0, errors.New("cannot seek a S3 writer")
	}
	// Make |offset| absolute.
	if whence == 0 {
	} else if whence == 1 {
		offset += f.readOffset
	} else if whence == 2 {
		return 0, errors.New("a S3 writer cannot seek relative to file end")
	}
	// If seeking before the current |readOffset|, re-open |f.readCloser|.
	if offset < f.readOffset && (f.err == nil || f.err == io.EOF) {
		f.readCloser.Close()
		f.readCloser = nil
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
func (f *s3File) Readdir(count int) ([]os.FileInfo, error) {
	if !f.IsDir() {
		return nil, errors.New("not a directory")
	}

	// TODO(joshk): Streaming directory listings (e.g. on-demand calls to
	// s3.ListObjectsV2.) Right now, just call until all entries have been
	// returned, then iterate over the results in-memory.
	if len(f.results) == 0 {
		f.results, f.err = f.listObjects()
		if f.err != nil {
			return nil, f.err
		}
		f.resultsIndex = 0
	}

	var remain = f.results[f.resultsIndex:]
	var toReturn int
	if nRemain := len(remain); nRemain == 0 {
		return nil, io.EOF
	} else if count < 0 || count > nRemain {
		toReturn = nRemain
	} else {
		toReturn = count
	}

	f.resultsIndex += toReturn
	return remain[:toReturn], nil
}

// File interface method
func (f *s3File) ContentSignature() (string, error) {
	if f.object == nil {
		return "", os.ErrNotExist
	} else {
		return *f.object.ETag, nil
	}
}

// A s3File is also a os.FileInfo.
func (f *s3File) Stat() (os.FileInfo, error) {
	return f, nil
}

// os.FileInfo interface method.
func (f *s3File) Name() string {
	var name = *f.key

	if name == "" {
		return *f.bucket
	} else if name[len(name)-1] == '/' {
		name = name[:len(name)-1]
	}
	return name[strings.LastIndex(name, "/")+1:]
}

// os.FileInfo interface method. May return size of the compressed file,
// and not the actual bytestream length.
func (f *s3File) Size() int64 {
	if f.object != nil {
		return *f.object.Size
	}
	return 0
}

// os.FileInfo interface method.
func (f *s3File) Mode() os.FileMode {
	log.Panic("s3File does not implement Mode()")
	return 0
}

// os.FileInfo interface method.
func (f *s3File) ModTime() time.Time {
	if f.object != nil {
		return *f.object.LastModified
	}
	return time.Time{}
}

// os.FileInfo interface method.
func (f *s3File) IsDir() bool {
	return isBucketStoreDir(*f.key)
}

// os.FileInfo interface method.
func (f *s3File) Sys() interface{} {
	return nil
}

func (f *s3File) reader() (io.ReadCloser, error) {
	var err error
	if f.readCloser == nil {
		var params = s3.GetObjectInput{
			Bucket: f.bucket,
			Key:    f.key,
		}

		var resp *s3.GetObjectOutput
		resp, err = f.svc.GetObject(&params)
		if err == nil {
			f.readCloser = resp.Body
		}
	}

	return f.readCloser, err
}

// Uploads the current content of the spool as a multipart chunk to S3.
// Records the ETag of the uploaded part on success, then resets the spool and
// increments the part number to prepare for the next series of writes.
func (f *s3File) uploadSpool() error {
	var buf = &f.spool
	var partNumber int64 = int64(len(f.uploadedParts)) + 1
	var params = s3.UploadPartInput{
		Bucket:        f.bucket,
		Key:           f.key,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      f.uploadId,
		Body:          bytes.NewReader(buf.Bytes()),
		ContentLength: aws.Int64(int64(buf.Len())),
	}

	var err error
	var resp *s3.UploadPartOutput
	resp, err = f.svc.UploadPart(&params)

	if err != nil {
		f.abortUpload()
		return err
	} else if resp.ETag == nil {
		f.abortUpload()
		err = errors.New("expected ETag on successful UploadPart")
		return err
	}

	// On success, rotate the spool.
	buf.Reset()

	// Record response ETag. We must provide it to complete the upload.
	f.uploadedParts = append(f.uploadedParts, &s3.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: aws.Int64(partNumber),
	})

	return nil
}

func (f *s3File) abortUpload() {
	// Abort the upload, if possible. Returned |err| is for the initial
	// part upload that we just tried -- abort error is logged but
	// swallowed.
	var abortParams = s3.AbortMultipartUploadInput{
		Bucket:   f.bucket,
		Key:      f.key,
		UploadId: f.uploadId,
	}

	_, abortErr := f.svc.AbortMultipartUpload(&abortParams)
	log.WithField("err", abortErr).Warn("failed to abort multipart upload")
}

func (f *s3File) listObjects() ([]os.FileInfo, error) {
	if !f.IsDir() {
		return nil, errors.New("not a directory")
	}

	// TODO(joshk): Instead, store *s3.Object? Problem is, prefixes aren't
	// objects, but Readdir() must return prefixes as well as objects.
	var results []os.FileInfo
	var continuation *string

	for {
		var listParams = s3.ListObjectsV2Input{
			Bucket:            f.bucket,
			ContinuationToken: continuation,
			Delimiter:         aws.String("/"),
			MaxKeys:           s3MaxListObjectsKeys,
			Prefix:            f.key,
		}

		objects, err := f.svc.ListObjectsV2(&listParams)
		if err != nil {
			return nil, err
		}

		for i := range objects.CommonPrefixes {
			if *objects.CommonPrefixes[i].Prefix == "/" {
				// Don't tangle with any prefix which actually contains the
				// delimiter as part of its name. Just skip it.
				continue
			}
			var f = &s3File{
				svc:    f.svc,
				bucket: f.bucket,
				key:    objects.CommonPrefixes[i].Prefix,
			}
			results = append(results, f)
		}
		for i := range objects.Contents {
			if objects.Contents[i].Key == f.key {
				// A directory can also have a regular file with the same path.
				// Skip it, should one occur.
				continue
			}
			var f = &s3File{
				svc:    f.svc,
				bucket: f.bucket,
				key:    objects.Contents[i].Key,
				object: objects.Contents[i],
			}
			results = append(results, f)
		}

		if objects.IsTruncated != nil && !*objects.IsTruncated {
			break
		}

		continuation = objects.NextContinuationToken
	}

	return results, nil
}

// Common source of truth to define what constitutes a *s3File being used for
// upload purposes.
func (f *s3File) isUpload() bool {
	return f.uploadId != nil
}
