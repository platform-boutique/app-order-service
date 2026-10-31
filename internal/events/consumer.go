package events

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Record is one fetched Kafka record. HighWatermark is the partition's end offset when it was fetched.
type Record struct {
	Topic         string
	Partition     int32
	Offset        int64
	Key           []byte
	Value         []byte
	HighWatermark int64

	raw any // the client's own record (needed to commit), nil in tests
}

// ErrSourceClosed is returned by Poll once the source has been closed.
var ErrSourceClosed = errors.New("record source closed")

// Source is where records come from and where offsets go back to. The Kafka implementation is NewKafkaSource;
// tests use a fake.
type Source interface {
	// Poll blocks until records are available (in partition order) or ctx is done. A non-nil error with no records
	// means "the broker is trouble, back off and ask again".
	Poll(ctx context.Context) ([]Record, error)
	// Finish commits the offsets after the given records (the ones that were handled, in order, possibly none) and
	// must be called after every Poll that returned records.
	Finish(ctx context.Context, handled []Record) error
	// Close leaves the group and releases the connections.
	Close()
}

// Consumer runs the poll -> handle -> commit loop.
type Consumer struct {
	Source  Source
	Handler Handler
	Log     *logrus.Logger
	Metrics *Metrics

	// Backoff is the wait before retry number n (1, 2, ...) of one record, or before polling again after n failed
	// polls in a row. Default: 0.5 s doubling up to 30 s.
	Backoff func(n int) time.Duration
	// Sleep waits d or until ctx is done (false then). Default: a real timer. Tests inject a fake.
	Sleep func(ctx context.Context, d time.Duration) bool
	// AttemptTimeout bounds one handling attempt (default 3 s). The attempt is not cancelled by shutdown: a record
	// that is being handled when SIGTERM arrives is finished and committed.
	AttemptTimeout time.Duration
	// CommitTimeout bounds the offset commit, also during shutdown (default 3 s).
	CommitTimeout time.Duration
}

// DefaultBackoff doubles from 500 ms up to 30 s.
func DefaultBackoff(n int) time.Duration {
	d := 500 * time.Millisecond
	for i := 1; i < n && d < 30*time.Second; i++ {
		d *= 2
	}
	return min(d, 30*time.Second)
}

func realSleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (c *Consumer) defaults() {
	if c.Backoff == nil {
		c.Backoff = DefaultBackoff
	}
	if c.Sleep == nil {
		c.Sleep = realSleep
	}
	if c.AttemptTimeout == 0 {
		c.AttemptTimeout = 3 * time.Second
	}
	if c.CommitTimeout == 0 {
		c.CommitTimeout = 3 * time.Second
	}
}

// Run consumes until ctx is cancelled, then closes the source and returns. It never returns because of a broker or
// handler problem.
func (c *Consumer) Run(ctx context.Context) {
	c.defaults()
	defer c.Source.Close()
	pollFailures := 0
	for ctx.Err() == nil {
		recs, err := c.Source.Poll(ctx)
		if ctx.Err() != nil || errors.Is(err, ErrSourceClosed) {
			return
		}
		if err != nil && len(recs) == 0 {
			pollFailures++
			c.Log.Warnf("kafka poll failed (%d in a row), backing off: %v", pollFailures, err)
			if !c.Sleep(ctx, c.Backoff(pollFailures)) {
				return
			}
			continue
		}
		pollFailures = 0
		if len(recs) == 0 {
			continue
		}
		handled := make([]Record, 0, len(recs))
		for _, rec := range recs {
			if !c.handleRecord(ctx, rec) {
				break // shutdown while this record was still failing: it is not committed
			}
			handled = append(handled, rec)
		}
		c.commit(ctx, handled)
		for _, h := range handled {
			c.Metrics.setLag(h.Partition, h.HighWatermark-(h.Offset+1))
		}
	}
}

func (c *Consumer) commit(ctx context.Context, handled []Record) {
	// not derived from ctx: the commit after the last handled record must still go out during shutdown
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.CommitTimeout)
	defer cancel()
	if err := c.Source.Finish(cctx, handled); err != nil {
		// no data is lost: the next commit covers these records, or after a restart they are redelivered and the
		// idempotent handler recognises them
		c.Metrics.commits.WithLabelValues("error").Inc()
		c.Log.Warnf("offset commit failed (%d records handled): %v", len(handled), err)
		return
	}
	if len(handled) > 0 {
		c.Metrics.commits.WithLabelValues("ok").Inc()
	}
}

// handleRecord returns true when the record is done (applied or deliberately skipped) and false when ctx ended
// while it was still failing.
func (c *Consumer) handleRecord(ctx context.Context, rec Record) bool {
	meta := Meta{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	l := c.Log.WithFields(logrus.Fields{"topic": rec.Topic, "partition": rec.Partition, "offset": rec.Offset})
	ev, err := Parse(rec.Value)
	if err != nil {
		reason := "malformed"
		if errors.Is(err, ErrUnsupportedSchema) {
			reason = "unsupported_schema"
		}
		c.Metrics.skipped.WithLabelValues(reason).Inc()
		l.Warnf("skipping record: %v", err) // the error has no payload in it
		return true
	}
	l = l.WithField("order_id", ev.OrderID)
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if !c.Sleep(ctx, c.Backoff(attempt)) {
				return false
			}
			c.Metrics.retried.Inc()
		}
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.AttemptTimeout)
		res, err := c.Handler.Handle(hctx, meta, ev)
		cancel()
		if err == nil {
			if res.Skipped() {
				c.Metrics.skipped.WithLabelValues(string(res)).Inc()
			} else {
				c.Metrics.processed.WithLabelValues(string(res)).Inc()
			}
			return true
		}
		c.Metrics.failed.Inc()
		l.Warnf("handling failed (attempt %d), will retry: %v", attempt+1, err)
		if ctx.Err() != nil {
			return false
		}
	}
}

// Group lets a caller wait for a Run that was started in a goroutine.
type Group struct {
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Start runs c in the background until Stop is called or ctx ends.
func Start(ctx context.Context, c *Consumer) *Group {
	ctx, cancel := context.WithCancel(ctx)
	g := &Group{cancel: cancel}
	g.wg.Add(1)
	go func() { defer g.wg.Done(); c.Run(ctx) }()
	return g
}

// Stop cancels the consumer and waits up to timeout for it to finish its current record, commit and close. It
// reports whether it finished in time.
func (g *Group) Stop(timeout time.Duration) bool {
	g.cancel()
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
