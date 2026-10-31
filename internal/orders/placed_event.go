package orders

import (
	"context"
	"errors"
	"fmt"

	pb "order-service/proto"
)

// ConfirmOutcome says what ConfirmFromEvent found and did.
type ConfirmOutcome string

const (
	// OutcomeConfirmed: the order was PENDING and is PLACED now (the reconciliation of a missed ConfirmOrder).
	OutcomeConfirmed ConfirmOutcome = "confirmed"
	// OutcomeAlreadyPlaced: the order was already confirmed with the same references (idempotent replay).
	OutcomeAlreadyPlaced ConfirmOutcome = "already_placed"
	// OutcomeUnknownOrder: no such order here (the event may come from another environment).
	OutcomeUnknownOrder ConfirmOutcome = "unknown_order"
	// OutcomeFailedOrder: the order is FAILED; it is never resurrected.
	OutcomeFailedOrder ConfirmOutcome = "failed_order"
	// OutcomeConflict: the order is PLACED with different tracking/transaction ids; nothing is applied.
	OutcomeConflict ConfirmOutcome = "conflict"
	// OutcomeInvalid: the references break the ConfirmOrder contract (same validation as the RPC); a retry cannot help.
	OutcomeInvalid ConfirmOutcome = "invalid"
)

// Ready reports whether the handlers are switched on (the database is migrated and reachable).
func (s *Service) Ready() bool { return s.ready.Load() }

// ConfirmFromEvent is the in-process twin of the ConfirmOrder RPC for the orders.placed consumer: the same
// validation and the same store transition, but with an outcome for every case that is not an error. A returned
// error is transient (database down, service not ready, timeout) and the caller must retry; it is never one of the
// outcomes above.
func (s *Service) ConfirmFromEvent(ctx context.Context, orderID, tracking, txn string) (ConfirmOutcome, error) {
	id, tracking, txn, err := ValidateConfirm(&pb.ConfirmOrderRequest{
		OrderId: orderID, ShippingTrackingId: tracking, PaymentTransactionId: txn,
	})
	if err != nil {
		return OutcomeInvalid, err
	}
	if !s.ready.Load() {
		return "", fmt.Errorf("%w: service not ready", ErrUnavailable)
	}
	// Look first, then confirm: Confirm alone cannot tell a replay from a first confirmation. If the order changes
	// between the two (a concurrent confirm or fail) the precondition error sends us round once more.
	for attempt := 0; attempt < 3; attempt++ {
		o, err := s.store.Get(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return OutcomeUnknownOrder, nil
		}
		if err != nil {
			return "", err
		}
		switch o.State {
		case StateFailed:
			return OutcomeFailedOrder, nil
		case StatePlaced:
			if o.TrackingID == tracking && o.TransactionID == txn {
				return OutcomeAlreadyPlaced, nil
			}
			return OutcomeConflict, nil
		}
		_, err = s.store.Confirm(ctx, id, tracking, txn, s.clock())
		switch {
		case err == nil:
			return OutcomeConfirmed, nil
		case errors.Is(err, ErrNotFound):
			return OutcomeUnknownOrder, nil
		case errors.Is(err, ErrPrecondition):
			continue
		default:
			return "", err
		}
	}
	return "", fmt.Errorf("order changed state repeatedly while confirming")
}
