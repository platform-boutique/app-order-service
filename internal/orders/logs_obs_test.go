package orders

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"order-service/internal/obs"
	pb "order-service/proto"

	"go.opentelemetry.io/otel/trace"
)

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, ln := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("log line is not JSON: %v: %s", err, ln)
		}
		out = append(out, m)
	}
	return out
}

// The handlers log with log.WithContext(ctx): inside a request the line carries the request's trace_id and
// span_id (and request_id); a call without a span has none. No e-mail address or session id is logged.
func TestHandlerLogsCarryTraceIDs(t *testing.T) {
	svc, st, _ := newTestService(t)
	var buf bytes.Buffer
	svc.log.SetFormatter(obs.NewFormatter("order-service", "test"))
	svc.log.SetOutput(&buf)

	tid, _ := trace.TraceIDFromHex(testTraceID)
	sid, _ := trace.SpanIDFromHex(testSpanID)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	ctx = obs.WithRequestID(ctx, "req-42")

	if _, err := svc.CreateOrder(ctx, validRequest()); err != nil {
		t.Fatal(err)
	}
	st.err = ErrUnavailable
	if _, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"}); err == nil {
		t.Fatal("want an error from the unavailable store")
	}
	lines := logLines(t, &buf)
	if len(lines) != 2 {
		t.Fatalf("got %d log lines: %s", len(lines), buf.String())
	}
	for _, m := range lines {
		if m["trace_id"] != testTraceID || m["span_id"] != testSpanID || m["request_id"] != "req-42" || m["service"] != "order-service" {
			t.Errorf("request-path line without ids: %v", m)
		}
	}
	if lines[1]["severity"] != "warning" {
		t.Errorf("unavailable store should be a warning: %v", lines[1])
	}
	for _, secret := range []string{"ada@example.com", "sess-1"} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("log leaks %q: %s", secret, buf.String())
		}
	}

	buf.Reset()
	st.err = nil
	if _, err := svc.CreateOrder(context.Background(), validRequest()); err != nil {
		t.Fatal(err)
	}
	for _, m := range logLines(t, &buf) {
		if _, ok := m["trace_id"]; ok {
			t.Errorf("trace_id outside a span: %v", m)
		}
		if _, ok := m["span_id"]; ok {
			t.Errorf("span_id outside a span: %v", m)
		}
	}
}
