package events

import (
	"context"
	"errors"

	"order-service/internal/orders"

	"github.com/sirupsen/logrus"
)

// Confirmer is the part of the order service the handler needs (implemented by *orders.Service).
type Confirmer interface {
	ConfirmFromEvent(ctx context.Context, orderID, tracking, txn string) (orders.ConfirmOutcome, error)
}

// Result is what happened to a record that was handled without an error.
type Result string

const (
	ResultConfirmed     Result = "confirmed"      // PENDING -> PLACED
	ResultAlreadyPlaced Result = "already_placed" // replay, nothing to do
	ResultUnknownOrder  Result = "unknown_order"  // skipped
	ResultFailedOrder   Result = "failed_order"   // skipped
	ResultConflict      Result = "conflict"       // skipped
	ResultInvalid       Result = "invalid"        // skipped
)

// Skipped is true for the results where the event was deliberately not applied.
func (r Result) Skipped() bool { return r != ResultConfirmed && r != ResultAlreadyPlaced }

// Meta locates a record in the log; it is all the logs may say about it besides the order id.
type Meta struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Handler handles one parsed event. An error means "transient, retry me"; it must never be returned for an event
// that can never succeed.
type Handler interface {
	Handle(ctx context.Context, m Meta, ev Event) (Result, error)
}

// ConfirmHandler applies order.placed events through the order service layer.
type ConfirmHandler struct {
	Orders Confirmer
	Log    *logrus.Logger
}

// Handle implements Handler.
func (h *ConfirmHandler) Handle(ctx context.Context, m Meta, ev Event) (Result, error) {
	l := h.Log.WithFields(logrus.Fields{"order_id": ev.OrderID, "partition": m.Partition, "offset": m.Offset})
	out, err := h.Orders.ConfirmFromEvent(ctx, ev.OrderID, ev.ShippingTrackingID, ev.PaymentTransactionID)
	if out == orders.OutcomeInvalid {
		l.Warnf("order.placed has invalid references, skipped: %v", err)
		return ResultInvalid, nil
	}
	if err != nil {
		return "", err
	}
	switch out {
	case orders.OutcomeConfirmed:
		l.Info("order.placed: order confirmed from the event (PENDING -> PLACED)")
		return ResultConfirmed, nil
	case orders.OutcomeAlreadyPlaced:
		l.Debug("order.placed: order already placed, nothing to do")
		return ResultAlreadyPlaced, nil
	case orders.OutcomeUnknownOrder:
		l.Warn("order.placed for an unknown order, skipped")
		return ResultUnknownOrder, nil
	case orders.OutcomeFailedOrder:
		l.Errorf("order.placed for a FAILED order order=%s", ev.OrderID)
		return ResultFailedOrder, nil
	case orders.OutcomeConflict:
		l.Error("order.placed carries different tracking/transaction ids than the placed order, not applied")
		return ResultConflict, nil
	}
	return "", errors.New("unexpected confirm outcome " + string(out))
}
