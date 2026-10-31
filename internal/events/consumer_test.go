package events

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"order-service/internal/orders"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const wait = 30 * time.Second // generous: the host clock jumps

func eventJSON(order string) []byte {
	return []byte(`{"event_id":"e-` + order + `","event_type":"order.placed","schema_version":1,"order_id":"` + order +
		`","email":"secret@example.com","shipping_address":{"street_address":"9 Secret Rd","city":"X","state":"","country":"","zip_code":0},` +
		`"items":[],"shipping_cost":{"currency_code":"USD","units":0,"nanos":0},"total":{"currency_code":"USD","units":1,"nanos":0},` +
		`"shipping_tracking_id":"TRK-` + order + `","payment_transaction_id":"TXN-` + order + `"}`)
}

func rec(partition int32, offset int64, value []byte) Record {
	return Record{Topic: Topic, Partition: partition, Offset: offset, Key: []byte("k"), Value: value, HighWatermark: offset + 3}
}

// fakeSource hands out the queued batches, then blocks until ctx ends (closing drained first).
type fakeSource struct {
	mu        sync.Mutex
	batches   [][]Record
	pollErrs  []error // returned (with no records) before the batches
	finishErr error
	committed [][]Record
	closed    bool
	drained   chan struct{}
	once      sync.Once
}

func newFakeSource(batches ...[]Record) *fakeSource {
	return &fakeSource{batches: batches, drained: make(chan struct{})}
}

func (f *fakeSource) Poll(ctx context.Context) ([]Record, error) {
	f.mu.Lock()
	if len(f.pollErrs) > 0 {
		err := f.pollErrs[0]
		f.pollErrs = f.pollErrs[1:]
		f.mu.Unlock()
		return nil, err
	}
	if len(f.batches) > 0 {
		b := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return b, nil
	}
	f.mu.Unlock()
	f.once.Do(func() { close(f.drained) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeSource) Finish(_ context.Context, handled []Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, append([]Record(nil), handled...))
	return f.finishErr
}

func (f *fakeSource) Close() { f.mu.Lock(); f.closed = true; f.mu.Unlock() }

func (f *fakeSource) commits() [][]Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]Record(nil), f.committed...)
}

// fakeHandler answers per order id from a script; the default is ResultConfirmed.
type fakeHandler struct {
	mu     sync.Mutex
	calls  []string
	script map[string][]func() (Result, error) // consumed one per call, the last one repeats
}

func (h *fakeHandler) Handle(_ context.Context, _ Meta, ev Event) (Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, ev.OrderID)
	s := h.script[ev.OrderID]
	if len(s) == 0 {
		return ResultConfirmed, nil
	}
	f := s[0]
	if len(s) > 1 {
		h.script[ev.OrderID] = s[1:]
	}
	return f()
}

func (h *fakeHandler) called() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...)
}

type fakeSleep struct {
	mu       sync.Mutex
	d        []time.Duration
	stopAt   int // when > 0 the n-th sleep reports "context ended" (false)
	onCancel func()
}

func (s *fakeSleep) Sleep(ctx context.Context, d time.Duration) bool {
	s.mu.Lock()
	s.d = append(s.d, d)
	n := len(s.d)
	s.mu.Unlock()
	if s.stopAt > 0 && n >= s.stopAt {
		if s.onCancel != nil {
			s.onCancel()
		}
		return false
	}
	return ctx.Err() == nil
}

func (s *fakeSleep) durations() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.d...)
}

type rig struct {
	src    *fakeSource
	h      *fakeHandler
	sl     *fakeSleep
	reg    *prometheus.Registry
	c      *Consumer
	logs   *bytes.Buffer
	cancel context.CancelFunc
	done   chan struct{}
}

func newRig(src *fakeSource, h *fakeHandler) *rig {
	reg := prometheus.NewRegistry()
	buf := &bytes.Buffer{}
	l := logrus.New()
	l.Out = buf
	l.SetLevel(logrus.DebugLevel)
	sl := &fakeSleep{}
	return &rig{src: src, h: h, sl: sl, reg: reg, logs: buf, done: make(chan struct{}),
		c: &Consumer{Source: src, Handler: h, Log: l, Metrics: NewMetrics(reg), Sleep: sl.Sleep}}
}

// run starts the consumer and returns when the source has handed out everything (or the context ended).
func (r *rig) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.sl.onCancel = cancel
	go func() { r.c.Run(ctx); close(r.done) }()
	select {
	case <-r.src.drained:
	case <-r.done:
	case <-time.After(wait):
		t.Fatal("consumer did not drain the source")
	}
}

func (r *rig) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(wait):
		t.Fatal("consumer did not stop")
	}
	if !r.src.closed {
		t.Error("source not closed on shutdown")
	}
}

func (r *rig) counter(t *testing.T, name string, labels ...string) float64 {
	t.Helper()
	mfs, err := r.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			for i := 0; i+1 < len(labels); i += 2 {
				found := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == labels[i] && lp.GetValue() == labels[i+1] {
						found = true
					}
				}
				if !found {
					continue next
				}
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

func offsets(rs []Record) []int64 {
	var o []int64
	for _, r := range rs {
		o = append(o, r.Offset)
	}
	return o
}

func TestConsumerHandlesAndCommitsAfterTheBatch(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 10, eventJSON("a")), rec(1, 4, eventJSON("b")), rec(0, 11, eventJSON("c"))})
	h := &fakeHandler{script: map[string][]func() (Result, error){"b": {func() (Result, error) { return ResultAlreadyPlaced, nil }}}}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)

	if got := h.called(); strings.Join(got, ",") != "a,b,c" {
		t.Errorf("handled %v", got)
	}
	commits := src.commits()
	if len(commits) != 1 || len(commits[0]) != 3 {
		t.Fatalf("commits: %v", commits)
	}
	if r.counter(t, "order_service_kafka_events_processed_total", "result", "confirmed") != 2 ||
		r.counter(t, "order_service_kafka_events_processed_total", "result", "already_placed") != 1 ||
		r.counter(t, "order_service_kafka_offset_commits_total", "result", "ok") != 1 {
		t.Errorf("metrics wrong:\n%s", dump(t, r.reg))
	}
	// lag = high watermark (offset+3) - (offset+1) = 2
	if got := r.counter(t, "order_service_kafka_consumer_lag", "partition", "1"); got != 2 {
		t.Errorf("lag partition 1 = %v", got)
	}
}

func dump(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	mfs, _ := reg.Gather()
	var sb strings.Builder
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			sb.WriteString(mf.GetName() + " " + m.String() + "\n")
		}
	}
	return sb.String()
}

func TestConsumerSkipsMalformedAndUnsupportedAndCommitsThem(t *testing.T) {
	src := newFakeSource([]Record{
		rec(0, 0, []byte("garbage")),
		rec(0, 1, []byte(`{"schema_version":2,"order_id":"x"}`)),
		rec(0, 2, eventJSON("ok")),
	})
	h := &fakeHandler{}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)

	if got := h.called(); len(got) != 1 || got[0] != "ok" {
		t.Errorf("handler saw %v", got)
	}
	if c := src.commits(); len(c) != 1 || len(c[0]) != 3 {
		t.Errorf("skipped records must be committed too: %v", c)
	}
	if r.counter(t, "order_service_kafka_events_skipped_total", "reason", "malformed") != 1 ||
		r.counter(t, "order_service_kafka_events_skipped_total", "reason", "unsupported_schema") != 1 {
		t.Errorf("metrics wrong:\n%s", dump(t, r.reg))
	}
	if !strings.Contains(r.logs.String(), "offset=0") || !strings.Contains(r.logs.String(), "partition=0") {
		t.Errorf("skip log lacks partition/offset: %s", r.logs.String())
	}
}

func TestConsumerSkippedResultsAreCountedAndCommitted(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 0, eventJSON("u")), rec(0, 1, eventJSON("f"))})
	h := &fakeHandler{script: map[string][]func() (Result, error){
		"u": {func() (Result, error) { return ResultUnknownOrder, nil }},
		"f": {func() (Result, error) { return ResultFailedOrder, nil }},
	}}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)
	if r.counter(t, "order_service_kafka_events_skipped_total", "reason", "unknown_order") != 1 ||
		r.counter(t, "order_service_kafka_events_skipped_total", "reason", "failed_order") != 1 {
		t.Errorf("metrics wrong:\n%s", dump(t, r.reg))
	}
	if c := src.commits(); len(c) != 1 || len(c[0]) != 2 {
		t.Errorf("commits %v", c)
	}
}

func TestConsumerRetriesTransientErrorsWithoutSkipping(t *testing.T) {
	boom := errors.New("database unavailable")
	attempts := 0
	src := newFakeSource([]Record{rec(0, 5, eventJSON("a")), rec(0, 6, eventJSON("b"))})
	h := &fakeHandler{script: map[string][]func() (Result, error){"a": {
		func() (Result, error) { attempts++; return "", boom },
		func() (Result, error) { attempts++; return "", boom },
		func() (Result, error) { attempts++; return "", boom },
		func() (Result, error) { attempts++; return ResultConfirmed, nil },
	}}}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)

	if got := strings.Join(h.called(), ","); got != "a,a,a,a,b" {
		t.Errorf("call order %s: b must wait for a, a must not be skipped", got)
	}
	if want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}; len(r.sl.durations()) != 3 ||
		r.sl.durations()[0] != want[0] || r.sl.durations()[1] != want[1] || r.sl.durations()[2] != want[2] {
		t.Errorf("backoff %v, want %v", r.sl.durations(), want)
	}
	if c := src.commits(); len(c) != 1 || fmt.Sprint(offsets(c[0])) != "[5 6]" {
		t.Errorf("commits %v", c)
	}
	if r.counter(t, "order_service_kafka_events_failed_total") != 3 || r.counter(t, "order_service_kafka_events_retried_total") != 3 ||
		r.counter(t, "order_service_kafka_events_processed_total", "result", "confirmed") != 2 {
		t.Errorf("metrics wrong:\n%s", dump(t, r.reg))
	}
	if strings.Contains(r.logs.String(), "secret@example.com") || strings.Contains(r.logs.String(), "9 Secret Rd") {
		t.Errorf("payload leaked into the logs: %s", r.logs.String())
	}
}

func TestDefaultBackoff(t *testing.T) {
	want := map[int]time.Duration{1: 500 * time.Millisecond, 2: time.Second, 3: 2 * time.Second, 7: 30 * time.Second, 50: 30 * time.Second}
	for n, d := range want {
		if got := DefaultBackoff(n); got != d {
			t.Errorf("DefaultBackoff(%d) = %v, want %v", n, got, d)
		}
	}
}

func TestConsumerShutdownWhileRetryingCommitsOnlyWhatWasHandled(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 1, eventJSON("a")), rec(0, 2, eventJSON("stuck")), rec(0, 3, eventJSON("c"))})
	h := &fakeHandler{script: map[string][]func() (Result, error){"stuck": {func() (Result, error) { return "", errors.New("db down") }}}}
	r := newRig(src, h)
	r.sl.stopAt = 2 // the second backoff wait is cut short by SIGTERM
	r.run(t)
	r.stop(t)

	if got := strings.Join(h.called(), ","); got != "a,stuck,stuck" {
		t.Errorf("calls %s (c must not be handled)", got)
	}
	c := src.commits()
	if len(c) != 1 || len(c[0]) != 1 || c[0][0].Offset != 1 {
		t.Errorf("only offset 1 may be committed, got %v", c)
	}
}

func TestConsumerFinishesTheRecordInFlightOnShutdown(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 1, eventJSON("a")), rec(0, 2, eventJSON("b"))})
	started := make(chan struct{})
	release := make(chan struct{})
	h := &fakeHandler{script: map[string][]func() (Result, error){"a": {func() (Result, error) {
		close(started)
		<-release
		return ResultConfirmed, nil
	}}}}
	r := newRig(src, h)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { r.c.Run(ctx); close(r.done) }()
	<-started
	cancel() // SIGTERM while "a" is being handled
	time.Sleep(10 * time.Millisecond)
	close(release)
	select {
	case <-r.done:
	case <-time.After(wait):
		t.Fatal("consumer did not stop")
	}
	c := src.commits()
	if len(c) != 1 || len(c[0]) != 1 || c[0][0].Offset != 1 {
		t.Errorf("record a was finished and must be committed (and b not): %v", c)
	}
	if got := h.called(); len(got) != 1 {
		t.Errorf("b must not be started after shutdown: %v", got)
	}
}

func TestConsumerBacksOffWhenPollFails(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 0, eventJSON("a"))})
	src.pollErrs = []error{errors.New("broker down"), errors.New("broker down")}
	h := &fakeHandler{}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)
	if d := r.sl.durations(); len(d) != 2 || d[0] != 500*time.Millisecond || d[1] != time.Second {
		t.Errorf("poll backoff %v", d)
	}
	if len(h.called()) != 1 {
		t.Errorf("calls %v", h.called())
	}
}

func TestConsumerSurvivesACommitFailure(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 0, eventJSON("a"))}, []Record{rec(0, 1, eventJSON("b"))})
	src.finishErr = errors.New("commit failed")
	h := &fakeHandler{}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)
	if strings.Join(h.called(), ",") != "a,b" || len(src.commits()) != 2 {
		t.Errorf("calls %v commits %v", h.called(), src.commits())
	}
	if r.counter(t, "order_service_kafka_offset_commits_total", "result", "error") != 2 {
		t.Errorf("metrics wrong:\n%s", dump(t, r.reg))
	}
}

func TestConsumerStopsWhenSourceCloses(t *testing.T) {
	src := newFakeSource()
	src.pollErrs = []error{ErrSourceClosed}
	r := newRig(src, &fakeHandler{})
	r.run(t)
	select {
	case <-r.done:
	case <-time.After(wait):
		t.Fatal("Run did not return")
	}
}

// --- the real handler on top of a fake service layer ---

type fakeConfirmer struct {
	out orders.ConfirmOutcome
	err error
	got []string
}

func (f *fakeConfirmer) ConfirmFromEvent(_ context.Context, id, trk, txn string) (orders.ConfirmOutcome, error) {
	f.got = append(f.got, id+"|"+trk+"|"+txn)
	return f.out, f.err
}

func TestConfirmHandlerMapsOutcomes(t *testing.T) {
	ev, err := Parse(eventJSON("ord-9"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		out     orders.ConfirmOutcome
		want    Result
		skipped bool
		log     string
		level   string
	}{
		{orders.OutcomeConfirmed, ResultConfirmed, false, "order confirmed from the event", "info"},
		{orders.OutcomeAlreadyPlaced, ResultAlreadyPlaced, false, "", ""},
		{orders.OutcomeUnknownOrder, ResultUnknownOrder, true, "unknown order", "warning"},
		{orders.OutcomeFailedOrder, ResultFailedOrder, true, "order.placed for a FAILED order order=ord-9", "error"},
		{orders.OutcomeConflict, ResultConflict, true, "different tracking/transaction ids", "error"},
		{orders.OutcomeInvalid, ResultInvalid, true, "invalid references", "warning"},
	}
	for _, c := range cases {
		t.Run(string(c.out), func(t *testing.T) {
			buf := &bytes.Buffer{}
			l := logrus.New()
			l.Out = buf
			l.Formatter = &logrus.JSONFormatter{}
			fc := &fakeConfirmer{out: c.out}
			if c.out == orders.OutcomeInvalid {
				fc.err = errors.New("invalid argument: shipping_tracking_id is required")
			}
			h := &ConfirmHandler{Orders: fc, Log: l}
			res, err := h.Handle(context.Background(), Meta{Topic: Topic, Partition: 2, Offset: 77}, ev)
			if err != nil || res != c.want || res.Skipped() != c.skipped {
				t.Fatalf("res=%q skipped=%v err=%v", res, res.Skipped(), err)
			}
			if len(fc.got) != 1 || fc.got[0] != "ord-9|TRK-ord-9|TXN-ord-9" {
				t.Errorf("service called with %v", fc.got)
			}
			out := buf.String()
			if c.log != "" && (!strings.Contains(out, c.log) || !strings.Contains(out, `"level":"`+c.level+`"`)) {
				t.Errorf("log %q missing %q at %s", out, c.log, c.level)
			}
			if c.log != "" && (!strings.Contains(out, `"offset":77`) || !strings.Contains(out, `"partition":2`) || !strings.Contains(out, `"order_id":"ord-9"`)) {
				t.Errorf("log lacks order_id/partition/offset: %s", out)
			}
			if strings.Contains(out, "secret@example.com") || strings.Contains(out, "Secret Rd") {
				t.Errorf("payload leaked: %s", out)
			}
		})
	}
}

func TestConfirmHandlerReturnsTransientErrors(t *testing.T) {
	ev, _ := Parse(eventJSON("ord-9"))
	boom := errors.New("db down")
	l := logrus.New()
	l.Out = io.Discard
	res, err := (&ConfirmHandler{Orders: &fakeConfirmer{err: boom}, Log: l}).Handle(context.Background(), Meta{}, ev)
	if !errors.Is(err, boom) || res != "" {
		t.Errorf("res=%q err=%v", res, err)
	}
}

func TestConsumerWaitsForAMissingTopicAtInfoLevelWithoutGrowingBackoff(t *testing.T) {
	src := newFakeSource([]Record{rec(0, 0, eventJSON("a"))})
	missing := fmt.Errorf("%w: orders.placed", ErrTopicNotAvailable)
	src.pollErrs = []error{missing, missing, missing}
	h := &fakeHandler{}
	r := newRig(src, h)
	r.run(t)
	r.stop(t)
	d := r.sl.durations()
	if len(d) != 3 || d[0] != 2*time.Second || d[1] != 2*time.Second || d[2] != 2*time.Second {
		t.Errorf("waits %v, want 3 x 2s", d)
	}
	if strings.Contains(r.logs.String(), "level=warning") || !strings.Contains(r.logs.String(), "waiting for topic") {
		t.Errorf("a missing topic must be logged at info only: %s", r.logs.String())
	}
	if len(h.called()) != 1 {
		t.Errorf("record after the topic appeared was not handled: %v", h.called())
	}
}
