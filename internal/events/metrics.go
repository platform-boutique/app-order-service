package events

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the consumer's counters on the service's /metrics registry.
type Metrics struct {
	processed *prometheus.CounterVec // result: confirmed | already_placed
	skipped   *prometheus.CounterVec // reason: malformed | unsupported_schema | unknown_order | failed_order | conflict | invalid
	failed    prometheus.Counter     // handling attempts that returned an error (each one is retried)
	retried   prometheus.Counter     // retry attempts started after a backoff
	commits   *prometheus.CounterVec // result: ok | error
	lag       *prometheus.GaugeVec   // partition: high watermark minus the next offset to read, as of the last handled record
}

// NewMetrics creates the collectors and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "order_service_kafka_events_processed_total",
			Help: "orders.placed events applied (or recognised as already applied), by result."}, []string{"result"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "order_service_kafka_events_skipped_total",
			Help: "orders.placed events skipped on purpose, by reason."}, []string{"reason"}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{Name: "order_service_kafka_events_failed_total",
			Help: "Handling attempts that failed with a transient error (the record is retried, never skipped)."}),
		retried: prometheus.NewCounter(prometheus.CounterOpts{Name: "order_service_kafka_events_retried_total",
			Help: "Retry attempts started after a backoff."}),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "order_service_kafka_offset_commits_total",
			Help: "Offset commits after a handled batch, by result."}, []string{"result"}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "order_service_kafka_consumer_lag",
			Help: "Records between the last handled record and the partition's high watermark, updated after each batch."}, []string{"partition"}),
	}
	reg.MustRegister(m.processed, m.skipped, m.failed, m.retried, m.commits, m.lag)
	return m
}

func (m *Metrics) setLag(partition int32, lag int64) {
	if lag < 0 {
		lag = 0
	}
	m.lag.WithLabelValues(strconv.Itoa(int(partition))).Set(float64(lag))
}
