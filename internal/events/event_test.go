package events

import (
	"errors"
	"strings"
	"testing"
)

const validEvent = `{
 "event_id":"7b4f8b0e-1111-5222-8333-444455556666","event_type":"order.placed","schema_version":1,
 "occurred_at":"2026-10-31T18:00:00.123Z","order_id":"ord-1","user_id":"u1","email":"customer@example.com",
 "shipping_address":{"street_address":"1 Main St","city":"London","state":"","country":"UK","zip_code":1234},
 "items":[{"product_id":"tee","name":"Tee","quantity":2,"unit_price":{"currency_code":"USD","units":12,"nanos":990000000},"picture":"/t.jpg"}],
 "shipping_cost":{"currency_code":"USD","units":5,"nanos":0},"total":{"currency_code":"USD","units":30,"nanos":980000000},
 "shipping_tracking_id":"TRK-1","payment_transaction_id":"TXN-1"}`

func TestParseValid(t *testing.T) {
	ev, err := Parse([]byte(validEvent))
	if err != nil {
		t.Fatal(err)
	}
	if ev.OrderID != "ord-1" || ev.ShippingTrackingID != "TRK-1" || ev.PaymentTransactionID != "TXN-1" || ev.SchemaVersion != 1 {
		t.Errorf("parsed: %+v", ev)
	}
	if len(ev.Items) != 1 || ev.Items[0].UnitPrice.Units != 12 || ev.Items[0].UnitPrice.Nanos != 990000000 || ev.Total.Units != 30 ||
		ev.ShippingAddress.ZipCode != 1234 || ev.EventID == "" || ev.OccurredAt == "" {
		t.Errorf("parsed: %+v", ev)
	}
}

func TestParseIgnoresUnknownFields(t *testing.T) {
	in := strings.Replace(validEvent, `"order_id":"ord-1",`, `"order_id":"ord-1","coupon":{"x":[1,2]},"future":"yes",`, 1)
	ev, err := Parse([]byte(in))
	if err != nil || ev.OrderID != "ord-1" {
		t.Fatalf("ev=%+v err=%v", ev, err)
	}
}

func TestParseSkips(t *testing.T) {
	for _, c := range []struct {
		name string
		in   string
		want error
	}{
		{"schema 2", strings.Replace(validEvent, `"schema_version":1`, `"schema_version":2`, 1), ErrUnsupportedSchema},
		{"schema 2 with changed field types", `{"schema_version":2,"order_id":["x"],"total":"17.99"}`, ErrUnsupportedSchema},
		{"schema 0", strings.Replace(validEvent, `"schema_version":1`, `"schema_version":0`, 1), ErrUnsupportedSchema},
		{"schema missing", strings.Replace(validEvent, `"schema_version":1,`, ``, 1), ErrUnsupportedSchema},
		{"schema a string", strings.Replace(validEvent, `"schema_version":1`, `"schema_version":"1"`, 1), ErrUnsupportedSchema},
		{"not json", `order placed!`, ErrMalformed},
		{"empty", ``, ErrMalformed},
		{"json array", `[1,2]`, ErrMalformed},
		{"truncated", validEvent[:60], ErrMalformed},
		{"wrong field type", strings.Replace(validEvent, `"order_id":"ord-1"`, `"order_id":42`, 1), ErrMalformed},
		{"no order id", strings.Replace(validEvent, `"order_id":"ord-1"`, `"order_id":""`, 1), ErrMalformed},
		{"other event type", strings.Replace(validEvent, `"event_type":"order.placed"`, `"event_type":"order.shipped"`, 1), ErrMalformed},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.in))
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if strings.Contains(err.Error(), "customer@example.com") || strings.Contains(err.Error(), "ord-1") {
				t.Errorf("error leaks payload: %v", err)
			}
		})
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if c := ConfigFromEnv(env(nil)); c.Enabled() || c.Group != "order-service" {
		t.Errorf("empty env: %+v", c)
	}
	if c := ConfigFromEnv(env(map[string]string{"KAFKA_BROKERS": " , "})); c.Enabled() {
		t.Errorf("blank brokers must be off: %+v", c)
	}
	c := ConfigFromEnv(env(map[string]string{"KAFKA_BROKERS": "a:9092, b:9092,", "KAFKA_CONSUMER_GROUP": "g2"}))
	if !c.Enabled() || len(c.Brokers) != 2 || c.Brokers[1] != "b:9092" || c.Group != "g2" {
		t.Errorf("config: %+v", c)
	}
}
