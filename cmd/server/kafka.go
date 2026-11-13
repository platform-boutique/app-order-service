package main

import (
	"context"
	"os"
	"time"

	"order-service/internal/events"
	"order-service/internal/orders"

	"github.com/prometheus/client_golang/prometheus"
)

// startKafka starts the orders.placed consumer when KAFKA_BROKERS is set and returns the function that stops it
// (finish the current record, commit, leave the group; at most 8 s). With KAFKA_BROKERS empty it does nothing and the
// returned function is a no-op. A consumer problem never stops the gRPC server or its health.
func startKafka(ctx context.Context, svc *orders.Service) (stop func()) {
	cfg := events.ConfigFromEnv(os.Getenv)
	if !cfg.Enabled() {
		log.Info("kafka consumer off — set KAFKA_BROKERS to enable")
		return func() {}
	}
	src, err := events.NewKafkaSource(cfg, log)
	if err != nil {
		log.Errorf("kafka consumer NOT started: %v", err)
		return func() {}
	}
	c := &events.Consumer{
		Source:  &gatedSource{Source: src, ready: svc.Ready},
		Handler: &events.ConfirmHandler{Orders: svc, Log: log},
		Log:     log,
		Group:   cfg.Group,
		Metrics: events.NewMetrics(prometheus.DefaultRegisterer),
	}
	g := events.Start(ctx, c)
	log.Infof("kafka consumer started: topic %s, group %s, %d broker(s)", events.Topic, cfg.Group, len(cfg.Brokers))
	return func() {
		if !g.Stop(8 * time.Second) {
			log.Warn("kafka consumer did not stop in time")
		}
	}
}

// gatedSource does not poll before the service is ready (database migrated), so records are not fetched just to
// fail on a database that is not there yet. The group membership only starts with the first poll.
type gatedSource struct {
	events.Source
	ready func() bool
}

func (g *gatedSource) Poll(ctx context.Context) ([]events.Record, error) {
	for !g.ready() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return g.Source.Poll(ctx)
}
