package azure

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/gorilla/schema"
	"go.gazette.dev/core/broker/stores/common"
)

// StoreQueryArgs contains fields that are parsed from the query arguments
// of an azure:// or azure-ad:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
}

// storeBase provides common Azure storage operations
type storeBase struct {
	args           StoreQueryArgs
	storageAccount string // Storage accounts in Azure are the equivalent to a "bucket" in S3
	container      string // In azure, blobs are stored inside of containers, which live inside accounts
	prefix         string // This is the path prefix for the blobs inside the container
	pipeline       pipeline.Pipeline
}

func (a *storeBase) Exists(ctx context.Context, path string) (bool, error) {
	var blobURL, err = a.buildBlobURL(path)
	if err != nil {
		return false, err
	}
	if _, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err == nil {
		return true, nil
	}
	if inner, ok := err.(azblob.StorageError); ok && inner.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return false, nil
	}
	return false, err
}

func (a *storeBase) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	var blobURL, err = a.buildBlobURL(path)
	if err != nil {
		return nil, err
	}
	download, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return download.Body(azblob.RetryReaderOptions{}), nil
}

func (a *storeBase) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	var blobURL, err = a.buildBlobURL(path)
	if err != nil {
		return err
	}
	var headers = azblob.BlobHTTPHeaders{}
	if contentEncoding != "" {
		headers.ContentEncoding = contentEncoding
	}
	// Azure SDK requires io.ReadSeeker, so we use io.NewSectionReader to adapt io.ReaderAt
	var sectionReader = io.NewSectionReader(content, 0, contentLength)
	_, err = blobURL.Upload(ctx, sectionReader, headers, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	return err
}

func (a *storeBase) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	prefix = a.args.RewritePath(a.prefix, prefix)

	var u, err = url.Parse(a.containerURL())
	if err != nil {
		return err
	}
	var containerURL = azblob.NewContainerURL(*u, a.pipeline)
	var options = azblob.ListBlobsSegmentOptions{Prefix: prefix}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		var segmentList, err = containerURL.ListBlobsFlatSegment(ctx, marker, options)
		if err != nil {
			return err
		}
		for _, blob := range segmentList.Segment.BlobItems {
			if strings.HasSuffix(blob.Name, "/") {
				continue // Ignore directory-like objects
			}
			// Return path relative to the listing prefix
			var relPath = strings.TrimPrefix(blob.Name, prefix)
			if err := callback(relPath, blob.Properties.LastModified); err != nil {
				return err
			}
		}
		marker = segmentList.NextMarker
	}
	return nil
}

func (a *storeBase) Remove(ctx context.Context, path string) error {
	var blobURL, err = a.buildBlobURL(path)
	if err != nil {
		return err
	}
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

func (a *storeBase) IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	if storageErr, ok := err.(azblob.StorageError); ok {
		switch storageErr.ServiceCode() {
		case azblob.ServiceCodeContainerNotFound,
			azblob.ServiceCodeContainerDisabled,
			azblob.ServiceCodeAccountIsDisabled:
			return true
		}

		if storageErr.Response() != nil {
			switch storageErr.Response().StatusCode {
			case http.StatusForbidden:
				return true
			}
		}
	}

	return false
}

func (a *storeBase) buildBlobURL(path string) (*azblob.BlockBlobURL, error) {
	var u, err = url.Parse(fmt.Sprint(a.containerURL(), "/", a.args.RewritePath(a.prefix, path)))
	if err != nil {
		return nil, err
	}
	var blobURL = azblob.NewBlockBlobURL(*u, a.pipeline)
	return &blobURL, nil
}

func azureStorageURL(storageAccount string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount)
}

func (a *storeBase) containerURL() string {
	return fmt.Sprintf("%s/%s", azureStorageURL(a.storageAccount), a.container)
}

func parseStoreArgs(ep *url.URL, args interface{}) error {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	if q, err := url.ParseQuery(ep.RawQuery); err != nil {
		return err
	} else if err = decoder.Decode(args, q); err != nil {
		return fmt.Errorf("parsing store URL arguments: %s", err)
	}
	return nil
}
