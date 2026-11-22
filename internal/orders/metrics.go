package orders

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Business counter (OBS-20: orders created and confirmed), next to the RED series of the gRPC middleware, the Kafka
// consumer series and the db_pool_* series. It counts what the database COMMITTED, so an idempotent replay
// (same CreateOrder content, repeated ConfirmOrder with the same references, an orders.placed event for an order
// that is already placed) and a rolled-back transaction are never counted. The label is a fixed small set.
var ordersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "order_service_orders_total",
	Help: "Order state changes committed to the database by event: created (new PENDING order), placed (PENDING to PLACED), failed (PENDING to FAILED).",
}, []string{"event"})

const (
	eventCreated = "created"
	eventPlaced  = "placed"
	eventFailed  = "failed"
)

func init() {
	// The series exist (at 0) from the start, so a rate() over them works before the first order.
	for _, e := range []string{eventCreated, eventPlaced, eventFailed} {
		ordersTotal.WithLabelValues(e)
	}
}

// countTransition counts the committed state change that produced an order in this state.
func countTransition(state string) {
	switch state {
	case StatePlaced:
		ordersTotal.WithLabelValues(eventPlaced).Inc()
	case StateFailed:
		ordersTotal.WithLabelValues(eventFailed).Inc()
	}
}
