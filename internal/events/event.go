// Package events consumes the orders.placed Kafka topic and turns each event into a ConfirmOrder.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Topic is the topic with the order.placed events (key = order id).
const Topic = "orders.placed"

const (
	eventTypePlaced = "order.placed"
	schemaVersion   = 1
)

// Parse errors. Both mean "skip the record, retrying cannot help".
var (
	ErrMalformed         = errors.New("malformed event")
	ErrUnsupportedSchema = errors.New("unsupported event schema")
)

// Money is the units+nanos amount used everywhere in the shop.
type Money struct {
	CurrencyCode string `json:"currency_code"`
	Units        int64  `json:"units"`
	Nanos        int32  `json:"nanos"`
}

// Address is the shipping address.
type Address struct {
	StreetAddress string `json:"street_address"`
	City          string `json:"city"`
	State         string `json:"state"`
	Country       string `json:"country"`
	ZipCode       int32  `json:"zip_code"`
}

// Item is one order line.
type Item struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Quantity  int32  `json:"quantity"`
	UnitPrice Money  `json:"unit_price"`
	Picture   string `json:"picture"`
}

// Event is the orders.placed value, schema version 1. Unknown fields are ignored. The consumer only acts on
// OrderID, ShippingTrackingID and PaymentTransactionID; the rest is parsed so that the struct documents the contract.
// Never log an Event: it carries the customer's email and address.
type Event struct {
	EventID              string  `json:"event_id"`
	EventType            string  `json:"event_type"`
	SchemaVersion        int     `json:"schema_version"`
	OccurredAt           string  `json:"occurred_at"`
	OrderID              string  `json:"order_id"`
	UserID               string  `json:"user_id"`
	Email                string  `json:"email"`
	ShippingAddress      Address `json:"shipping_address"`
	Items                []Item  `json:"items"`
	ShippingCost         Money   `json:"shipping_cost"`
	Total                Money   `json:"total"`
	ShippingTrackingID   string  `json:"shipping_tracking_id"`
	PaymentTransactionID string  `json:"payment_transaction_id"`
}

// Parse decodes one record value. The schema version is checked on its own first, so that a future version that
// changed other fields is reported as unsupported and not as malformed. The returned errors never contain the payload.
func Parse(value []byte) (Event, error) {
	var head struct {
		SchemaVersion json.RawMessage `json:"schema_version"`
	}
	if err := json.Unmarshal(value, &head); err != nil {
		return Event{}, fmt.Errorf("%w: not a JSON object", ErrMalformed)
	}
	var v int
	if len(head.SchemaVersion) == 0 || json.Unmarshal(head.SchemaVersion, &v) != nil || v != schemaVersion {
		return Event{}, fmt.Errorf("%w: schema_version missing, invalid or not %d", ErrUnsupportedSchema, schemaVersion)
	}
	var ev Event
	if err := json.Unmarshal(value, &ev); err != nil {
		return Event{}, fmt.Errorf("%w: fields have the wrong type", ErrMalformed)
	}
	if ev.EventType != eventTypePlaced {
		return Event{}, fmt.Errorf("%w: event_type is not %q", ErrMalformed, eventTypePlaced)
	}
	if ev.OrderID == "" {
		return Event{}, fmt.Errorf("%w: order_id is empty", ErrMalformed)
	}
	return ev, nil
}
