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
// (finish the current record, commit, leave the group; at most the given time, false = it did not finish). With
// KAFKA_BROKERS empty it does nothing and the returned function is a no-op. An invalid KAFKA_TOPIC_PREFIX is an error
// (the service does not start). A consumer problem later never stops the gRPC server or its health.
func startKafka(ctx context.Context, svc *orders.Service) (stop func(time.Duration) bool, err error) {
	cfg := events.ConfigFromEnv(os.Getenv)
	if !cfg.Enabled() {
		log.Info("kafka consumer off — set KAFKA_BROKERS to enable")
		return func(time.Duration) bool { return true }, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	src, err := events.NewKafkaSource(cfg, log)
	if err != nil {
		log.Errorf("kafka consumer NOT started: %v", err)
		return func(time.Duration) bool { return true }, nil
	}
	c := &events.Consumer{
		Source:  &gatedSource{Source: src, ready: svc.Ready},
		Handler: &events.ConfirmHandler{Orders: svc, Log: log},
		Log:     log,
		Group:   cfg.Group,
		Metrics: events.NewMetrics(prometheus.DefaultRegisterer),
	}
	g := events.Start(ctx, c)
	log.Infof("kafka consumer started: topic %s, group %s, %d broker(s); the topic is created if missing", cfg.Topic, cfg.Group, len(cfg.Brokers))
	return g.Stop, nil
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
