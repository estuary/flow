// Package envflagfactory contains common env flag definitions used across
// gazette tools. Use of them ensures the consistent naming of flags and
// environment variables, default values, and usage texts across multiple
// programs.
package envflagfactory

import "github.com/LiveRamp/gazette/envflag"

// NewGazetteServiceEndpoint defines the gazette service endpoint flag.
func NewGazetteServiceEndpoint() *string {
	return envflag.CommandLine.String(
		"gazette",
		"GAZETTE_SERVICE_ENDPOINT",
		"gazette.example:8081",
		"Gazette network service host:port.")
}

// NewEtcdServiceEndpoint defines the Etcd service endpoint flag.
func NewEtcdServiceEndpoint() *string {
	return envflag.CommandLine.String(
		"etcd",
		"ETCD_SERVICE_ENDPOINT",
		"etcd.example:2379",
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

// NewLogLevel defines the logrus log-level flag.
func NewLogLevel() *string {
	return envflag.CommandLine.String(
		"logLevel",
		"LOG_LEVEL",
		"info",
		"Logrus log-level.",
	)
}

// NewMetricsPath defines the HTTP resource path on which to fetch metrics.
func NewMetricsPath() *string {
	return envflag.CommandLine.String(
		"metricsPath",
		"METRICS_PATH",
		"/metrics",
		"The HTTP resource path on which to fetch metrics.",
	)
}

// NewMetricsPort defines the metrics port flag.
func NewMetricsPort() *string {
	return envflag.CommandLine.String(
		"metricsPort",
		"METRICS_PORT",
		":8090",
		"Metrics port.",
	)
}
