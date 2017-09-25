package gazette_test

import (
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/metrics"
)

func ExampleNewClient() {
	// Register collectors and attach handler to expose metrics to Prometheus.
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)
	http.Handle("/metrics", promhttp.Handler())

	// Get a client.
	client, err := gazette.NewClient("192.168.0.1:8081")
	if err != nil {
		logrus.WithField("err", err).Fatal("Failed to connect to gazette")
	}

	// Use client.
	client.Get(journal.ReadArgs{Journal: "a/journal", Offset: 1234})
}
