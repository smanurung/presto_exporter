[![GoDoc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/sonnythehottest/presto_exporter)
# presto_exporter
Prometheus exporter for prestodb

## Usage
```
./presto_exporter --presto-http-url=<scheme>://<presto-host>:<presto-port> --log-level=<log-level> --port <port>
```

## Example
```
./presto_exporter --presto-http-url=<scheme>://<presto-host>:<presto-port> --log-level=debug --port 9999
INFO[0000] running presto_exporter on port 9999
DEBU[0000] runningQueries: 12.000000, activeWorkers: 9.000000
DEBU[0001] runningQueries: 13.000000, activeWorkers: 9.000000
DEBU[0002] runningQueries: 13.000000, activeWorkers: 9.000000
```