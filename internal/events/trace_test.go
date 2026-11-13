package events

import (
	"context"
	"testing"

	"order-service/internal/obs"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type ctxHandler struct{ got trace.SpanContext }

func (h *ctxHandler) Handle(ctx context.Context, _ Meta, _ Event) (Result, error) {
	h.got = trace.SpanContextFromContext(ctx)
	return ResultConfirmed, nil
}

// OBS-61: the handler runs inside a CONSUMER span that is the child of the producer span named
// in the record's traceparent header; records without usable headers get a new root, never an error.
func TestHandleRecordContinuesTheProducersTrace(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	defer func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) }()

	// The producer side (checkout): a span and the headers it writes.
	pctx, root := otel.Tracer("t").Start(context.Background(), "PlaceOrder")
	var hs []obs.KafkaHeader
	_, prod := obs.ProducerSpan(pctx, Topic, &hs)
	prod.End()
	root.End()

	h := &ctxHandler{}
	c := &Consumer{Handler: h, Log: logrus.New(), Metrics: NewMetrics(prometheus.NewRegistry()), Group: "order-service"}
	r := rec(1, 7, eventJSON("o-1"))
	r.Topic = Topic
	r.Headers = hs
	if !c.handleRecord(context.Background(), r) {
		t.Fatal("record not handled")
	}
	if h.got.TraceID() != root.SpanContext().TraceID() {
		t.Fatalf("handler trace %s, want the producer's %s", h.got.TraceID(), root.SpanContext().TraceID())
	}
	var cons sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		if s.SpanKind() == trace.SpanKindConsumer {
			cons = s
		}
	}
	if cons == nil || cons.Parent().SpanID() != prod.SpanContext().SpanID() {
		t.Fatalf("consumer span missing or not a child of the producer span: %v", spans.Ended())
	}

	// No headers / garbage: still handled, as a new root trace.
	for name, bad := range map[string][]obs.KafkaHeader{"none": nil, "garbage": {{Key: "traceparent", Value: []byte("zz")}}} {
		h.got = trace.SpanContext{}
		r.Headers = bad
		if !c.handleRecord(context.Background(), r) {
			t.Fatalf("%s: record not handled", name)
		}
		if !h.got.IsValid() || h.got.TraceID() == root.SpanContext().TraceID() {
			t.Fatalf("%s: expected a fresh root trace, got %s", name, h.got.TraceID())
		}
	}
}
