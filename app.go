package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// decide which metrics type to use, e.g. counter, gauge, histogram, or summary
	runningQueries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "running_queries",
		Help: "Number of running queries",
	})

	activeWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_workers",
		Help: "Number of active presto workers",
	})

	port      = kingpin.Flag("port", "port to bind the app to").Default("9988").String()
	prestoURL = kingpin.Flag("presto-http-url", "Presto HTTP URL").Required().String()
	logLevel  = kingpin.Flag("log-level", "log level to use").Default("info").String()
)

// Stats includes attributes of presto statistics
type Stats struct {
	RunningQueries float64 `json:"runningQueries"`
	ActiveWorkers  float64 `json:"activeWorkers"`
}

func init() {
	prometheus.MustRegister(runningQueries, activeWorkers)
}

func main() {
	kingpin.Parse()

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("invalid logrus level: %v", err)
	}
	log.SetLevel(lvl)

	go poll()

	http.Handle("/metrics", promhttp.Handler())

	log.Infof("running presto_exporter on port %s", *port)
	log.Print(http.ListenAndServe(":"+*port, nil))
}

func poll() error {
	// hit presto stats endpoint and decide which metrics to record
	httpClient := http.Client{
		Timeout: 5 * time.Second,
	}

	uri := fmt.Sprintf("%s/v1/cluster", *prestoURL)

	r, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		log.Fatalf("failed to create get request to %s", uri)
	}

	var (
		stat Stats
	)

	for {
		resp, err := httpClient.Do(r)
		if err != nil {
			log.Errorf("failed to send http request: %v", err)
			continue
		}
		defer resp.Body.Close()

		encoded, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("failed to read response body: %v", err)
			continue
		}

		err = json.Unmarshal(encoded, &stat)
		if err != nil {
			log.Errorf("failed to unmarshal presto stats response (%s): %v", encoded, err)
			continue
		}

		log.Debugf("runningQueries: %f, activeWorkers: %f", stat.RunningQueries, stat.ActiveWorkers)

		runningQueries.Set(stat.RunningQueries)
		activeWorkers.Set(stat.ActiveWorkers)

		n := time.Duration(1)
		time.Sleep(n * time.Second)
	}
}
