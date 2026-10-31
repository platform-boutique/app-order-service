package orders

import (
	"context"
	"errors"
	"testing"
)

func TestConfirmFromEvent(t *testing.T) {
	ctx := context.Background()
	create := func(t *testing.T) (*Service, *memStore) {
		svc, st, _ := newTestService(t)
		if _, err := svc.CreateOrder(ctx, validRequest()); err != nil {
			t.Fatal(err)
		}
		return svc, st
	}
	state := func(t *testing.T, st *memStore) Order {
		t.Helper()
		o, err := st.Get(ctx, "ord-1")
		if err != nil {
			t.Fatal(err)
		}
		return o
	}

	t.Run("pending becomes placed", func(t *testing.T) {
		svc, st := create(t)
		out, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1")
		if err != nil || out != OutcomeConfirmed {
			t.Fatalf("out=%q err=%v", out, err)
		}
		if o := state(t, st); o.State != StatePlaced || o.TrackingID != "TRK-1" || o.TransactionID != "TXN-1" || o.ConfirmedAt == nil {
			t.Errorf("order after confirm: %+v", o)
		}
	})

	t.Run("replay is a no-op and keeps the confirmation time", func(t *testing.T) {
		svc, st := create(t)
		if _, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1"); err != nil {
			t.Fatal(err)
		}
		first := *state(t, st).ConfirmedAt
		out, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1")
		if err != nil || out != OutcomeAlreadyPlaced {
			t.Fatalf("out=%q err=%v", out, err)
		}
		if !state(t, st).ConfirmedAt.Equal(first) {
			t.Error("replay moved confirmed_at")
		}
	})

	t.Run("already placed through the RPC then the event: no-op", func(t *testing.T) {
		svc, _ := create(t)
		if _, err := svc.ConfirmOrder(ctx, confirmReq("ord-1")); err != nil {
			t.Fatal(err)
		}
		out, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1")
		if err != nil || out != OutcomeAlreadyPlaced {
			t.Fatalf("out=%q err=%v", out, err)
		}
	})

	t.Run("different tracking id on a placed order is not applied", func(t *testing.T) {
		svc, st := create(t)
		if _, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1"); err != nil {
			t.Fatal(err)
		}
		out, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-OTHER", "TXN-1")
		if err != nil || out != OutcomeConflict {
			t.Fatalf("out=%q err=%v", out, err)
		}
		if o := state(t, st); o.TrackingID != "TRK-1" {
			t.Errorf("tracking id overwritten: %q", o.TrackingID)
		}
	})

	t.Run("failed order is never resurrected", func(t *testing.T) {
		svc, st := create(t)
		if _, err := st.Fail(ctx, "ord-1", "card declined", svc.clock()); err != nil {
			t.Fatal(err)
		}
		out, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1")
		if err != nil || out != OutcomeFailedOrder {
			t.Fatalf("out=%q err=%v", out, err)
		}
		if o := state(t, st); o.State != StateFailed || o.TrackingID != "" {
			t.Errorf("failed order changed: %+v", o)
		}
	})

	t.Run("unknown order", func(t *testing.T) {
		svc, _, _ := newTestService(t)
		out, err := svc.ConfirmFromEvent(ctx, "nope", "TRK-1", "TXN-1")
		if err != nil || out != OutcomeUnknownOrder {
			t.Fatalf("out=%q err=%v", out, err)
		}
	})

	t.Run("same validation as the RPC", func(t *testing.T) {
		svc, st := create(t)
		for _, c := range []struct{ id, trk, txn string }{
			{"ord-1", "", "TXN-1"}, {"ord-1", "TRK-1", ""}, {"bad id!", "TRK-1", "TXN-1"}, {"", "TRK-1", "TXN-1"},
		} {
			out, err := svc.ConfirmFromEvent(ctx, c.id, c.trk, c.txn)
			if out != OutcomeInvalid || !errors.Is(err, ErrInvalid) {
				t.Errorf("%+v: out=%q err=%v", c, out, err)
			}
		}
		if state(t, st).State != StatePending {
			t.Error("an invalid event changed the order")
		}
	})

	t.Run("database errors and not-ready are errors, not outcomes", func(t *testing.T) {
		svc, st := create(t)
		st.err = ErrUnavailable
		out, err := svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1")
		if out != "" || !errors.Is(err, ErrUnavailable) {
			t.Fatalf("out=%q err=%v", out, err)
		}
		st.err = nil
		svc.SetReady(false)
		out, err = svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1")
		if out != "" || !errors.Is(err, ErrUnavailable) {
			t.Fatalf("not ready: out=%q err=%v", out, err)
		}
		svc.SetReady(true)
		if out, err = svc.ConfirmFromEvent(ctx, "ord-1", "TRK-1", "TXN-1"); err != nil || out != OutcomeConfirmed {
			t.Fatalf("after recovery: out=%q err=%v", out, err)
		}
	})
}
