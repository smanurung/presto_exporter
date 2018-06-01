package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/sonnythehottest/presto_exporter/cluster"
	"github.com/sonnythehottest/presto_exporter/query"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	port      = kingpin.Flag("port", "port to bind the app to").Default("9988").String()
	prestoURL = kingpin.Flag("presto-http-url", "Presto HTTP URL").Required().String()
	logLevel  = kingpin.Flag("log-level", "log level to use").Default("info").String()
)

// Poller abstract the behaviour of a poll structure
type Poller interface {
	Poll()
}

func main() {
	kingpin.Parse()

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("invalid logrus level: %v", err)
	}
	log.SetLevel(lvl)

	go cluster.NewPoller(*prestoURL).Poll()
	go query.NewPoller(*prestoURL).Poll()

	http.Handle("/metrics", promhttp.Handler())

	log.Infof("running presto_exporter on port %s", *port)
	log.Print(http.ListenAndServe(":"+*port, nil))
}
