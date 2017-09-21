// Package envflagfactory contains common env flag definitions used across
// gazette tools. Use of them ensures the consistent naming of flags and
// environment variables, default values, and usage texts across multiple
// programs.
package envflagfactory

import "github.com/pippio/gazette/envflag"

// NewGazetteServiceEndpoint defines the gazette service endpoint flag.
func NewGazetteServiceEndpoint() *string {
	return envflag.CommandLine.ServiceEndpoint(
		"gazette",
		"127.0.0.1:8081",
		"Gazette network service host:port.")
}

// NewEtcdServiceEndpoint defines the Etcd service endpoint flag.
func NewEtcdServiceEndpoint() *string {
	return envflag.CommandLine.ServiceEndpoint(
		"etcd",
		"127.0.0.1:2379",
		"Etcd network service host:port.")
}

// NewCloudFSURL defines the cloudFS URL flag.
func NewCloudFSURL() *string {
	return envflag.CommandLine.String(
		"cloudFS",
		"CLOUD_FS_URL",
		"file:///cloud-fs",
		"URL parameterizing the cloud filesystem to use.")
}
