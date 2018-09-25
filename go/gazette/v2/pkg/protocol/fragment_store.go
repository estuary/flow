package protocol

import (
	"net/url"
)

// FragmentStore defines a storage backend base path for Journal Fragments.
// It is a URL, where the scheme defines the storage backend service. As
// FragmentStores "root" remote storage locations of fragments, their path
// component must end in a trailing slash.
//
// FragmentStore implementations may support additional configuration which
// can be declared via URL query arguments. The meaning of these query
// arguments and values are specific to the store in question; consult the
// store implementation to see properties available for configuration.
//
// Currently supported schemes are `gs` for Google Cloud Storage, `s3` for
// Amazon S3, and `file` for a local file-system / NFS mount. Eg:
//
//  * s3://bucket-name/a/sub-path/?profile=a-shared-credentials-profile
//  * gs://bucket-name/a/sub-path/?
//  * file:///a/local/volume/mount
//
type FragmentStore string

// Validate returns an error if the FragmentStore is not well-formed.
func (fs FragmentStore) Validate() error {
	var _, err = fs.parse()
	return err
}

// URL returns the FragmentStore as a URL. The FragmentStore must Validate, or URL panics.
func (fs FragmentStore) URL() *url.URL {
	if url, err := fs.parse(); err == nil {
		return url
	} else {
		panic(err.Error())
	}
}

func (fs FragmentStore) parse() (*url.URL, error) {
	var url, err = url.Parse(string(fs))
	if err != nil {
		return nil, &ValidationError{Err: err}
	} else if !url.IsAbs() {
		return nil, NewValidationError("not absolute (%s)", fs)
	}

	switch url.Scheme {
	case "s3", "gs":
		if url.Host == "" {
			return nil, NewValidationError("missing bucket (%s)", fs)
		}
	case "file":
		if url.Host != "" {
			return nil, NewValidationError("file scheme cannot have host (%s)", fs)
		}
	default:
		return nil, NewValidationError("invalid scheme (%s)", url.Scheme)
	}

	if path := url.Path; path[len(path)-1] != '/' {
		return nil, NewValidationError("path component doesn't end in '/' (%s)", url.Path)
	}
	return url, nil
}

func fragmentStoresEq(a, b []FragmentStore) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
