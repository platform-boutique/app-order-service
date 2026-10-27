package orders

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

func placedAt(t time.Time) Order {
	return Order{State: StatePlaced, ConfirmedAt: &t}
}

func TestScheduleBoundaries(t *testing.T) {
	st := DefaultStages // 1m / 3m / 6m
	o := placedAt(t0)
	cases := []struct {
		age  time.Duration
		want Status
	}{
		{-5 * time.Second, StatusPlaced}, // clock behind the confirmation: age zero
		{0, StatusPlaced},
		{59*time.Second + 999*time.Millisecond, StatusPlaced},
		{time.Minute, StatusProcessing},
		{2*time.Minute + 59*time.Second, StatusProcessing},
		{3 * time.Minute, StatusShipped},
		{5*time.Minute + 59*time.Second, StatusShipped},
		{6 * time.Minute, StatusDelivered},
		{24 * time.Hour * 365, StatusDelivered},
	}
	for _, c := range cases {
		p := st.Schedule(o, t0.Add(c.age))
		if p.Status != c.want {
			t.Errorf("age %v: %s, want %s", c.age, p.Status, c.want)
		}
		if !p.PlacedAt.Equal(t0) || !p.ProcessingAt.Equal(t0.Add(time.Minute)) || !p.ShippedAt.Equal(t0.Add(3*time.Minute)) || !p.DeliveredAt.Equal(t0.Add(6*time.Minute)) {
			t.Errorf("age %v: stage timestamps %v %v %v %v", c.age, p.PlacedAt, p.ProcessingAt, p.ShippedAt, p.DeliveredAt)
		}
	}
}

func TestPendingAndFailedNeverAdvance(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  Status
	}{{StatePending, StatusPending}, {StateFailed, StatusFailed}} {
		for _, age := range []time.Duration{0, time.Hour, 1000 * time.Hour} {
			p := DefaultStages.Schedule(Order{State: tc.state}, t0.Add(age))
			if p.Status != tc.want || p.PlacedAt != nil || p.ProcessingAt != nil || p.ShippedAt != nil || p.DeliveredAt != nil {
				t.Errorf("%s after %v: %+v", tc.state, age, p)
			}
		}
	}
}

func TestStagesValidate(t *testing.T) {
	if err := DefaultStages.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, s := range []Stages{
		{0, time.Minute, 2 * time.Minute},
		{-time.Second, time.Minute, 2 * time.Minute},
		{time.Minute, time.Minute, 2 * time.Minute},
		{time.Minute, 3 * time.Minute, 3 * time.Minute},
		{3 * time.Minute, time.Minute, 6 * time.Minute},
	} {
		if s.Validate() == nil {
			t.Errorf("%+v accepted", s)
		}
	}
}

func TestApplyConfirm(t *testing.T) {
	pending := Order{NewOrder: NewOrder{ID: "o"}, State: StatePending}
	placed, changed, err := ApplyConfirm(pending, "trk", "txn", t0)
	if err != nil || !changed || placed.State != StatePlaced || placed.TrackingID != "trk" || placed.TransactionID != "txn" || !placed.ConfirmedAt.Equal(t0) {
		t.Fatalf("confirm: %+v %v %v", placed, changed, err)
	}
	if pending.ConfirmedAt != nil || pending.State != StatePending {
		t.Error("ApplyConfirm modified its input")
	}
	// retry with the same references later: unchanged, confirmation time stays
	again, changed, err := ApplyConfirm(placed, "trk", "txn", t0.Add(time.Hour))
	if err != nil || changed || !again.ConfirmedAt.Equal(t0) {
		t.Errorf("idempotent retry: %+v %v %v", again, changed, err)
	}
	// different references
	if _, _, err := ApplyConfirm(placed, "trk2", "txn", t0); !errors.Is(err, ErrPrecondition) {
		t.Errorf("different tracking id: %v", err)
	}
	// failed orders cannot be confirmed
	failed, _, _ := ApplyFail(pending, "card declined", t0)
	if _, _, err := ApplyConfirm(failed, "trk", "txn", t0); !errors.Is(err, ErrPrecondition) {
		t.Errorf("confirm failed order: %v", err)
	}
}

func TestApplyFail(t *testing.T) {
	pending := Order{NewOrder: NewOrder{ID: "o"}, State: StatePending}
	failed, changed, err := ApplyFail(pending, "card declined", t0)
	if err != nil || !changed || failed.State != StateFailed || failed.FailureReason != "card declined" || !failed.FailedAt.Equal(t0) {
		t.Fatalf("fail: %+v %v %v", failed, changed, err)
	}
	again, changed, err := ApplyFail(failed, "other reason", t0.Add(time.Hour))
	if err != nil || changed || again.FailureReason != "card declined" {
		t.Errorf("second fail must keep the first reason: %+v %v %v", again, changed, err)
	}
	placed, _, _ := ApplyConfirm(pending, "trk", "txn", t0)
	if _, _, err := ApplyFail(placed, "late", t0); !errors.Is(err, ErrPrecondition) {
		t.Errorf("fail placed order: %v", err)
	}
}
