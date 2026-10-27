package orders

import (
	"errors"
	"fmt"
	"time"
)

// Stored states of an order. The states shown to clients (PROCESSING, SHIPPED, DELIVERED) are not stored,
// they are computed from ConfirmedAt by Schedule.
const (
	StatePending = "PENDING"
	StatePlaced  = "PLACED"
	StateFailed  = "FAILED"
)

var (
	// ErrNotFound: no such order (or it belongs to someone else).
	ErrNotFound = errors.New("order not found")
	// ErrConflict: the order id exists with different content.
	ErrConflict = errors.New("order already exists with different content")
	// ErrPrecondition: the order is in a state that does not allow the call.
	ErrPrecondition = errors.New("order state does not allow this")
)

func precondition(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrPrecondition, fmt.Sprintf(format, args...))
}

// Order is a stored order.
type Order struct {
	NewOrder
	State         string // StatePending, StatePlaced or StateFailed
	FailureReason string
	TrackingID    string
	TransactionID string
	Fingerprint   string
	CreatedAt     time.Time
	ConfirmedAt   *time.Time
	FailedAt      *time.Time
}

// ApplyConfirm is the PENDING -> PLACED transition. changed is false when the order was already
// confirmed with the same references (idempotent retry). Both the Postgres store and the test
// store use it, so the rules live in one place.
func ApplyConfirm(o Order, tracking, txn string, now time.Time) (out Order, changed bool, err error) {
	switch o.State {
	case StatePlaced:
		if o.TrackingID == tracking && o.TransactionID == txn {
			return o, false, nil
		}
		return o, false, precondition("order %s is already placed with different tracking/transaction ids", o.ID)
	case StateFailed:
		return o, false, precondition("order %s has failed and cannot be confirmed", o.ID)
	}
	o.State = StatePlaced
	o.TrackingID = tracking
	o.TransactionID = txn
	t := now
	o.ConfirmedAt = &t
	return o, true, nil
}

// ApplyFail is the PENDING -> FAILED transition. An order that already failed is returned unchanged
// (the first reason stays); a placed order cannot fail.
func ApplyFail(o Order, reason string, now time.Time) (out Order, changed bool, err error) {
	switch o.State {
	case StateFailed:
		return o, false, nil
	case StatePlaced:
		return o, false, precondition("order %s is already placed and cannot be failed", o.ID)
	}
	o.State = StateFailed
	o.FailureReason = reason
	t := now
	o.FailedAt = &t
	return o, true, nil
}

// Stages holds the offsets after confirmation at which the shown status advances.
type Stages struct {
	Processing time.Duration // PLACED -> PROCESSING
	Shipped    time.Duration // PROCESSING -> SHIPPED
	Delivered  time.Duration // SHIPPED -> DELIVERED
}

// DefaultStages are the demo defaults: 1 minute, 3 minutes, 6 minutes after confirmation.
var DefaultStages = Stages{Processing: time.Minute, Shipped: 3 * time.Minute, Delivered: 6 * time.Minute}

// Validate requires 0 < Processing < Shipped < Delivered.
func (s Stages) Validate() error {
	if s.Processing <= 0 || s.Shipped <= s.Processing || s.Delivered <= s.Shipped {
		return fmt.Errorf("stage offsets must satisfy 0 < processing (%v) < shipped (%v) < delivered (%v)", s.Processing, s.Shipped, s.Delivered)
	}
	return nil
}

// Status is the status shown to clients.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusPlaced     Status = "PLACED"
	StatusProcessing Status = "PROCESSING"
	StatusShipped    Status = "SHIPPED"
	StatusDelivered  Status = "DELIVERED"
	StatusFailed     Status = "FAILED"
)

// Progress is the computed view of an order at one instant.
type Progress struct {
	Status Status
	// The moments each stage is (or will be) reached; nil for PENDING and FAILED orders.
	PlacedAt, ProcessingAt, ShippedAt, DeliveredAt *time.Time
}

// Schedule computes the shown status at time now. No state is stored and nothing runs in the background:
// PENDING and FAILED never advance; a PLACED order advances with now - ConfirmedAt. A clock that runs
// slightly behind the confirmation time (replica skew) counts as age zero.
func (s Stages) Schedule(o Order, now time.Time) Progress {
	switch o.State {
	case StatePending:
		return Progress{Status: StatusPending}
	case StateFailed:
		return Progress{Status: StatusFailed}
	}
	placed := *o.ConfirmedAt
	proc, shipped, delivered := placed.Add(s.Processing), placed.Add(s.Shipped), placed.Add(s.Delivered)
	p := Progress{PlacedAt: &placed, ProcessingAt: &proc, ShippedAt: &shipped, DeliveredAt: &delivered}
	switch {
	case !now.Before(delivered):
		p.Status = StatusDelivered
	case !now.Before(shipped):
		p.Status = StatusShipped
	case !now.Before(proc):
		p.Status = StatusProcessing
	default:
		p.Status = StatusPlaced
	}
	return p
}
