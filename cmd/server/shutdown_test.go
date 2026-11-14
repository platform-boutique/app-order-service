package main

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func quietLog() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// fakeClock hands out timers that fire when the test says so. No real time passes.
type fakeClock struct {
	mu     sync.Mutex
	timers map[time.Duration][]chan time.Time
	asked  chan time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{timers: map[time.Duration][]chan time.Time{}, asked: make(chan time.Duration, 16)}
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	c.timers[d] = append(c.timers[d], ch)
	c.mu.Unlock()
	c.asked <- d
	return ch
}

func (c *fakeClock) fire(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.timers[d] {
		ch <- time.Time{}
	}
}

func TestBudgetsFitInTenSeconds(t *testing.T) {
	if sum := drainBudget + poolCloseBudget + obsFlushBudget; sum >= shutdownBudget {
		t.Fatalf("drain %s + pool %s + flush %s = %s must leave room inside %s", drainBudget, poolCloseBudget, obsFlushBudget, sum, shutdownBudget)
	}
	if shutdownBudget > 10*time.Second {
		t.Fatalf("CODE-04 allows 10 s, shutdownBudget is %s", shutdownBudget)
	}
}

func TestShutdownCleanRunForcesNothing(t *testing.T) {
	var forced, consumerAsked, pooled atomic.Int32
	clock := newFakeClock()
	s := &shutdownSteps{
		GracefulStop: func() {},
		ForceStop:    func() { forced.Add(1) },
		StopConsumer: func(d time.Duration) bool { consumerAsked.Store(int32(d / time.Second)); return true },
		ClosePool:    func() { pooled.Add(1) },
		After:        clock.After,
		Log:          quietLog(),
	}
	s.run()
	if forced.Load() != 0 || pooled.Load() != 1 || consumerAsked.Load() != int32(drainBudget/time.Second) {
		t.Fatalf("forced=%d pooled=%d consumerBudget=%ds", forced.Load(), pooled.Load(), consumerAsked.Load())
	}
	s.run() // second call does nothing
	if pooled.Load() != 1 {
		t.Fatal("shutdown ran twice")
	}
}

// A gRPC call that never ends, a consumer that never stops and a pool that never closes: each is abandoned at its
// budget and the steps still return.
func TestShutdownAbandonsEverythingThatHangs(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	var forced atomic.Int32
	clock := newFakeClock()
	s := &shutdownSteps{
		GracefulStop: func() { <-block },
		ForceStop:    func() { forced.Add(1) },
		StopConsumer: func(time.Duration) bool { <-block; return false },
		ClosePool:    func() { <-block },
		After:        clock.After,
		Log:          quietLog(),
	}
	done := make(chan struct{})
	go func() { s.run(); close(done) }()

	if d := <-clock.asked; d != drainBudget {
		t.Fatalf("first timer = %s, want the drain budget %s", d, drainBudget)
	}
	select {
	case <-done:
		t.Fatal("returned before the drain budget ran out")
	default:
	}
	clock.fire(drainBudget)
	if d := <-clock.asked; d != poolCloseBudget {
		t.Fatalf("second timer = %s, want the pool budget %s", d, poolCloseBudget)
	}
	clock.fire(poolCloseBudget)
	<-done // no real timeout here: the test would hang (and fail by go test's own timeout) if run never returned
	if forced.Load() != 1 {
		t.Fatalf("ForceStop called %d times, want 1", forced.Load())
	}
}

func TestShutdownWithoutConsumer(t *testing.T) {
	clock := newFakeClock()
	var pooled atomic.Int32
	s := &shutdownSteps{GracefulStop: func() {}, ForceStop: func() {}, ClosePool: func() { pooled.Add(1) }, After: clock.After, Log: quietLog()}
	s.run()
	if pooled.Load() != 1 {
		t.Fatal("pool not closed")
	}
}

func TestWatchdogExitsAfterTheBudget(t *testing.T) {
	clock := newFakeClock()
	exited := make(chan int, 1)
	watchdog(clock.After, func(code int) { exited <- code }, quietLog())
	if d := <-clock.asked; d != shutdownBudget {
		t.Fatalf("watchdog timer = %s, want %s", d, shutdownBudget)
	}
	select {
	case c := <-exited:
		t.Fatalf("exited early with %d", c)
	default:
	}
	clock.fire(shutdownBudget)
	if code := <-exited; code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
}

func TestStartKafkaOffWithoutBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	stop, err := startKafka(t.Context(), nil)
	if err != nil || !stop(time.Second) {
		t.Fatalf("err=%v", err)
	}
}

func TestStartKafkaRejectsABadPrefix(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_TOPIC_PREFIX", "dev/x")
	if _, err := startKafka(t.Context(), nil); err == nil {
		t.Fatal("a prefix with '/' must stop the start")
	}
}
