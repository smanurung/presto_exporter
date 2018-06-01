package query

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// Poller includes attributes of query poller
type Poller struct {
	httpClient         *http.Client
	statsURL           string
	queryElapsedTime   prometheus.Histogram
	queryExecutionTime prometheus.Histogram
	sleepDuration      time.Duration
}

// Stats includes attributes of query execution info
type Stats struct {
	ElapsedTime   string `json:"elapsedTime"`
	CreateTime    string `json:"createTime"`
	EndTime       string `json:"endTime"`
	ExecutionTime string `json:"executionTime"`
}

// Info includes all information related to the query
type Info struct {
	QueryID    string `json:"queryId"`
	Query      string `json:"query"`
	QueryStats Stats  `json:"queryStats"`
}

// NewPoller initialized query poller.
func NewPoller(prestoURL string) *Poller {
	queryElapsedTime := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "query_elapsed_time_seconds",
		Help: "Duration of query elapsed time in seconds.",
	})
	queryExecutionTime := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "query_execution_time_seconds",
		Help: "Duration of query execution time in seconds.",
	})

	prometheus.MustRegister(queryElapsedTime, queryExecutionTime)

	return &Poller{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		statsURL:           fmt.Sprintf("%s/v1/query", prestoURL),
		queryElapsedTime:   queryElapsedTime,
		queryExecutionTime: queryExecutionTime,
		sleepDuration:      time.Minute,
	}
}

// Poll triggers query poller to retrieve query stats from presto
func (p *Poller) Poll() {
	r, err := http.NewRequest("GET", p.statsURL, nil)
	if err != nil {
		log.Fatalf("failed to create get request to %s", p.statsURL)
	}

	var (
		resp               *http.Response
		infoList           []Info
		elapsed, execution time.Duration
		endTime            time.Time
	)

	for {
		log.Debugf("retrieving query stats..")

		resp, err = p.httpClient.Do(r)
		if err != nil {
			log.Errorf("failed to send http request: %v", err)
			continue
		}

		err = json.NewDecoder(resp.Body).Decode(&infoList)
		if err != nil {
			resp.Body.Close()
			log.Errorf("failed to unmarshal presto query stats response: %v", err)
			continue
		}
		resp.Body.Close()

		for _, info := range infoList {
			// skip stats if it doesn't have endtime (perhaps the job hasn't finished yet).
			if info.QueryStats.EndTime == "" {
				continue
			}

			// we only care of queries that finished in the last `sleepDuration` duration.
			// Query stats older than that should have been exported in the previous poll.
			// Btw, presto time is in format: 2018-06-01T13:28:02.405Z
			endTime, err = time.Parse(time.RFC3339Nano, info.QueryStats.EndTime)
			if err != nil {
				log.Errorf("failed to parse query endTime %s: %v", info.QueryStats.EndTime, err)
				continue
			}

			if time.Since(endTime).Seconds() > p.sleepDuration.Seconds() {
				continue
			}

			elapsed, err = time.ParseDuration(info.QueryStats.ElapsedTime)
			if err != nil {
				log.Errorf("failed to parse elapsed time %s: %v", info.QueryStats.ElapsedTime, err)
				continue
			}

			execution, err = time.ParseDuration(info.QueryStats.ExecutionTime)
			if err != nil {
				log.Errorf("failed to parse execution time %s: %v", info.QueryStats.ExecutionTime, err)
				continue
			}

			log.Debugf("elapsedTime: (%s, %.0f), executionTime: (%s, %.0f)", info.QueryStats.ElapsedTime, elapsed.Seconds(), info.QueryStats.ExecutionTime, execution.Seconds())

			p.queryElapsedTime.Observe(elapsed.Seconds())
			p.queryExecutionTime.Observe(execution.Seconds())
		}

		time.Sleep(p.sleepDuration)
	}
}
