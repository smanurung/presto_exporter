package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// Poller includes attributes for cluster poller.
type Poller struct {
	httpClient     *http.Client
	statsURL       string
	runningQueries prometheus.Gauge
	activeWorkers  prometheus.Gauge
}

// Stats includes attributes of presto statistics
type Stats struct {
	RunningQueries float64 `json:"runningQueries"`
	ActiveWorkers  float64 `json:"activeWorkers"`
}

// NewPoller initialized cluster poller
func NewPoller(prestoURL string) *Poller {

	runningQueries := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "running_queries",
		Help: "Number of running queries",
	})

	activeWorkers := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_workers",
		Help: "Number of active presto workers",
	})

	prometheus.MustRegister(runningQueries, activeWorkers)

	return &Poller{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		statsURL:       fmt.Sprintf("%s/v1/cluster", prestoURL),
		runningQueries: runningQueries,
		activeWorkers:  activeWorkers,
	}
}

// Poll triggers cluster poll to retrieve stats from presto query stats URL
func (p *Poller) Poll() {
	// hit presto stats endpoint and decide which metrics to record
	r, err := http.NewRequest("GET", p.statsURL, nil)
	if err != nil {
		log.Fatalf("failed to create get request to %s", p.statsURL)
	}

	// avoid allocation inside for-loop (space complexity reason).
	var (
		stat Stats
		resp *http.Response
	)

	for {
		log.Debugf("retrieving cluter stats..")

		resp, err = p.httpClient.Do(r)
		if err != nil {
			log.Errorf("failed to send http request: %v", err)
			continue
		}

		err = json.NewDecoder(resp.Body).Decode(&stat)
		if err != nil {
			resp.Body.Close()
			log.Errorf("failed to unmarshal presto stats response: %v", err)
			continue
		}
		resp.Body.Close()

		log.Debugf("runningQueries: %f, activeWorkers: %f", stat.RunningQueries, stat.ActiveWorkers)

		p.runningQueries.Set(stat.RunningQueries)
		p.activeWorkers.Set(stat.ActiveWorkers)

		time.Sleep(10 * time.Second)
	}
}
