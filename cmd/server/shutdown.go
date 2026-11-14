package main

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// CODE-04: after SIGTERM the process exits within 10 s in total, also with a consumer that is stuck, a database that
// does not answer and a collector that is blackholed. The budgets add up to 9 s; shutdownBudget is the hard stop
// that fires if something not covered below hangs anyway.
const (
	drainBudget     = 5 * time.Second // in-flight gRPC calls and the Kafka consumer's current record, in parallel
	poolCloseBudget = 1 * time.Second // pgxpool.Close waits for connections that are still checked out
	obsFlushBudget  = 3 * time.Second // telemetry flush (obs.Options.ShutdownTimeout), runs after run() returned
	shutdownBudget  = 10 * time.Second
)

// shutdownSteps are the stops that run after the shutdown signal.
type shutdownSteps struct {
	GracefulStop func()                               // gRPC: returns when the in-flight calls are done
	ForceStop    func()                               // gRPC: closes what is left
	StopConsumer func(d time.Duration) bool           // Kafka consumer: finish the record, commit, leave the group; false = too slow
	ClosePool    func()                               // database pool
	After        func(time.Duration) <-chan time.Time // time.After; tests inject a fake clock
	Log          *logrus.Logger

	once sync.Once
}

// run drains gRPC and the consumer in parallel, for at most drainBudget, then closes the pool for at most
// poolCloseBudget. It is safe to call more than once; only the first call does anything.
func (s *shutdownSteps) run() {
	s.once.Do(s.do)
}

func (s *shutdownSteps) do() {
	grpcDone := make(chan struct{})
	go func() { s.GracefulStop(); close(grpcDone) }()
	consumerDone := make(chan bool, 1)
	if s.StopConsumer != nil {
		go func() { consumerDone <- s.StopConsumer(drainBudget) }()
	} else {
		consumerDone <- true
	}

	timeout := s.After(drainBudget)
	grpcFinished, consumerFinished := false, false
	for !grpcFinished || !consumerFinished {
		select {
		case <-grpcDone:
			grpcFinished = true
			grpcDone = nil
		case ok := <-consumerDone:
			consumerFinished = true
			consumerDone = nil
			if !ok {
				s.Log.Warn("kafka consumer did not stop in time; its last record is redelivered and skipped as a duplicate")
			}
		case <-timeout:
			if !grpcFinished {
				s.Log.Warnf("graceful stop timed out after %s, closing remaining connections", drainBudget)
				s.ForceStop()
			}
			if !consumerFinished {
				s.Log.Warnf("kafka consumer still running after %s, going on without it", drainBudget)
			}
			grpcFinished, consumerFinished = true, true
		}
	}

	poolDone := make(chan struct{})
	go func() { s.ClosePool(); close(poolDone) }()
	select {
	case <-poolDone:
	case <-s.After(poolCloseBudget):
		s.Log.Warnf("database pool still has connections checked out after %s, going on", poolCloseBudget)
	}
}

// watchdog ends the process with exit code 1 when the whole shutdown (this is started at the signal and not cancelled:
// the process ends when main returns) takes longer than shutdownBudget.
func watchdog(after func(time.Duration) <-chan time.Time, exit func(int), log *logrus.Logger) {
	timeout := after(shutdownBudget)
	go func() {
		<-timeout
		log.Errorf("shutdown did not finish within %s, exiting", shutdownBudget)
		exit(1)
	}()
}
