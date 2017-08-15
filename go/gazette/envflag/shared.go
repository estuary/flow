package envflag

// This file contains shared envflags used by Gazette that should have
// consistent names, default values, and usage text.

// TODO(rupert): This doesn't really belong in the envflag package. As these
// flags are meant to be used by command line tools, perhaps this belongs in
// gazette/tool.

// NewGazetteServiceEndpoint defines the gazette service endpoint flag.
func NewGazetteServiceEndpoint() *string {
	return ServiceEndpoint("gazette", "127.0.0.1:8081", "Gazette network service host:port")
}

// NewEtcdServiceEndpoint defines the Etcd service endpoint flag.
func NewEtcdServiceEndpoint() *string {
	return ServiceEndpoint("etcd", "127.0.0.1:2379", "Etcd network service host:port")
}

// NewCloudFSURL defines the cloudFS URL flag.
func NewCloudFSURL() *string {
	return String("cloudFS", "CLOUD_FS_URL", "file:///cloud-fs", "URL parameterizing the cloud filesystem to use.")
}
