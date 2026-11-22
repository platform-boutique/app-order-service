//go:build integration

package integration

import (
	"context"
	"testing"

	pb "order-service/proto"

	"github.com/prometheus/client_golang/prometheus"
)

// ordersCount reads order_service_orders_total{event} from the default registry (what /metrics serves).
func ordersCount(t *testing.T, event string) float64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != "order_service_orders_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "event" && l.GetValue() == event {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("order_service_orders_total{event=%q} not found", event)
	return 0
}

// OBS-20: orders created and confirmed are counted once per committed state change, never for replays.
func TestOrdersAreCountedOncePerCommittedChange(t *testing.T) {
	svc, _, _ := setup(t)
	ctx := context.Background()
	c0, p0, f0 := ordersCount(t, "created"), ordersCount(t, "placed"), ordersCount(t, "failed")

	for i := 0; i < 2; i++ { // the second call is an idempotent replay: not counted again
		if _, err := svc.CreateOrder(ctx, request("m-1", "sess-m1", "")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.CreateOrder(ctx, request("m-2", "sess-m2", "")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // the second confirm is an idempotent replay: not counted again
		if _, err := svc.ConfirmOrder(ctx, confirm("m-1")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ { // the second fail returns the failed order unchanged: not counted again
		if _, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "m-2", Reason: "card declined"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.ConfirmOrder(ctx, &pb.ConfirmOrderRequest{OrderId: "m-2", ShippingTrackingId: "T", PaymentTransactionId: "X"}); err == nil {
		t.Fatal("a failed order must not be confirmable") // refused transition: not counted
	}

	for event, want := range map[string]float64{"created": 2, "placed": 1, "failed": 1} {
		var got float64
		switch event {
		case "created":
			got = ordersCount(t, event) - c0
		case "placed":
			got = ordersCount(t, event) - p0
		case "failed":
			got = ordersCount(t, event) - f0
		}
		if got != want {
			t.Errorf("event %s: +%v, want +%v", event, got, want)
		}
	}
}
