package orders

import (
	"errors"
	"strings"
	"testing"

	pb "order-service/proto"
)

func TestValidateNewOrderOK(t *testing.T) {
	req := validRequest()
	req.OrderId = "  ord-1  "
	req.Email = " ada@example.com "
	req.Items[0].Name = "  Tee  "
	n, err := ValidateNewOrder(req)
	if err != nil {
		t.Fatal(err)
	}
	if n.ID != "ord-1" || n.Email != "ada@example.com" || n.Items[0].Name != "Tee" || len(n.Items) != 2 {
		t.Errorf("not normalised: %+v", n)
	}
	if n.Total != (Money{"USD", 50, 230_000_000}) {
		t.Errorf("total %+v", n.Total)
	}
	// an unset shipping_cost means zero shipping
	req = validRequest()
	req.ShippingCost = nil
	req.Total = &pb.Money{CurrencyCode: "USD", Units: 45, Nanos: 480_000_000}
	n, err = ValidateNewOrder(req)
	if err != nil || n.ShippingCost != (Money{Currency: "USD"}) {
		t.Errorf("no shipping: %+v %v", n.ShippingCost, err)
	}
}

func TestValidateNewOrderRejects(t *testing.T) {
	long := func(n int) string { return strings.Repeat("a", n) }
	manyItems := func(n int) []*pb.OrderItem {
		out := make([]*pb.OrderItem, n)
		for i := range out {
			out[i] = &pb.OrderItem{ProductId: "p", Name: "P", Quantity: 1, UnitPrice: &pb.Money{CurrencyCode: "USD", Units: 1}}
		}
		return out
	}
	cases := []struct {
		name string
		mut  func(r *pb.CreateOrderRequest)
	}{
		{"no order id", func(r *pb.CreateOrderRequest) { r.OrderId = " " }},
		{"order id too long", func(r *pb.CreateOrderRequest) { r.OrderId = long(65) }},
		{"order id slash", func(r *pb.CreateOrderRequest) { r.OrderId = "a/b" }},
		{"order id space", func(r *pb.CreateOrderRequest) { r.OrderId = "a b" }},
		{"order id leading dash", func(r *pb.CreateOrderRequest) { r.OrderId = "-a" }},
		{"order id non-ascii", func(r *pb.CreateOrderRequest) { r.OrderId = "ordér" }},
		{"no session", func(r *pb.CreateOrderRequest) { r.SessionId = "" }},
		{"session too long", func(r *pb.CreateOrderRequest) { r.SessionId = long(129) }},
		{"user too long", func(r *pb.CreateOrderRequest) { r.UserId = long(129) }},
		{"session NUL", func(r *pb.CreateOrderRequest) { r.SessionId = "a\x00b" }},
		{"session bad utf8", func(r *pb.CreateOrderRequest) { r.SessionId = "a\xffb" }},
		{"no email", func(r *pb.CreateOrderRequest) { r.Email = "" }},
		{"email without at", func(r *pb.CreateOrderRequest) { r.Email = "ada.example.com" }},
		{"email with name", func(r *pb.CreateOrderRequest) { r.Email = "Ada <ada@example.com>" }},
		{"email too long", func(r *pb.CreateOrderRequest) { r.Email = long(250) + "@e.co" }},
		{"no address", func(r *pb.CreateOrderRequest) { r.ShippingAddress = nil }},
		{"no street", func(r *pb.CreateOrderRequest) { r.ShippingAddress.StreetAddress = "" }},
		{"street too long", func(r *pb.CreateOrderRequest) { r.ShippingAddress.StreetAddress = long(201) }},
		{"no city", func(r *pb.CreateOrderRequest) { r.ShippingAddress.City = "" }},
		{"no country", func(r *pb.CreateOrderRequest) { r.ShippingAddress.Country = "" }},
		{"state too long", func(r *pb.CreateOrderRequest) { r.ShippingAddress.State = long(101) }},
		{"negative zip", func(r *pb.CreateOrderRequest) { r.ShippingAddress.ZipCode = -1 }},
		{"no items", func(r *pb.CreateOrderRequest) { r.Items = nil }},
		{"too many items", func(r *pb.CreateOrderRequest) {
			r.Items = manyItems(101)
			r.Total = &pb.Money{CurrencyCode: "USD", Units: 105}
			r.ShippingCost.Units, r.ShippingCost.Nanos = 4, 750_000_000
		}},
		{"nil item", func(r *pb.CreateOrderRequest) { r.Items[1] = nil }},
		{"item no product", func(r *pb.CreateOrderRequest) { r.Items[0].ProductId = "" }},
		{"item no name", func(r *pb.CreateOrderRequest) { r.Items[0].Name = "  " }},
		{"item name too long", func(r *pb.CreateOrderRequest) { r.Items[0].Name = long(201) }},
		{"picture too long", func(r *pb.CreateOrderRequest) { r.Items[0].Picture = long(501) }},
		{"quantity 0", func(r *pb.CreateOrderRequest) { r.Items[0].Quantity = 0 }},
		{"quantity negative", func(r *pb.CreateOrderRequest) { r.Items[0].Quantity = -2 }},
		{"quantity 1001", func(r *pb.CreateOrderRequest) { r.Items[0].Quantity = 1001 }},
		{"no unit price", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice = nil }},
		{"zero unit price", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice = &pb.Money{CurrencyCode: "USD"} }},
		{"negative units", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice.Units = -1 }},
		{"negative nanos", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice.Nanos = -1 }},
		{"nanos too big", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice.Nanos = 1_000_000_000 }},
		{"units too big", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice.Units = MaxUnits + 1 }},
		{"lower-case currency", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice.CurrencyCode = "usd" }},
		{"currency length", func(r *pb.CreateOrderRequest) { r.Items[0].UnitPrice.CurrencyCode = "USDX" }},
		{"mixed item currencies", func(r *pb.CreateOrderRequest) { r.Items[1].UnitPrice.CurrencyCode = "EUR" }},
		{"shipping other currency", func(r *pb.CreateOrderRequest) { r.ShippingCost.CurrencyCode = "EUR" }},
		{"total other currency", func(r *pb.CreateOrderRequest) { r.Total.CurrencyCode = "EUR" }},
		{"no total", func(r *pb.CreateOrderRequest) { r.Total = nil }},
		{"zero total", func(r *pb.CreateOrderRequest) { r.Total = &pb.Money{CurrencyCode: "USD"} }},
		{"total too low", func(r *pb.CreateOrderRequest) { r.Total.Nanos -= 10_000_000 }},
		{"total too high", func(r *pb.CreateOrderRequest) { r.Total.Units++ }},
		{"total without shipping", func(r *pb.CreateOrderRequest) { r.ShippingCost = nil }},
		{"negative shipping", func(r *pb.CreateOrderRequest) { r.ShippingCost.Units = -4 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := validRequest()
			c.mut(r)
			if _, err := ValidateNewOrder(r); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestValidateNewOrderLimitsAccepted(t *testing.T) {
	// exactly at the limits: 100 lines of 1000 x 1,000,000,000.999999999 must not overflow
	r := validRequest()
	r.OrderId = strings.Repeat("a", 64)
	r.SessionId = strings.Repeat("s", 128)
	r.UserId = strings.Repeat("u", 128)
	r.ShippingAddress.State = strings.Repeat("x", 100)
	r.Items = nil
	for i := 0; i < 100; i++ {
		r.Items = append(r.Items, &pb.OrderItem{ProductId: "p", Name: strings.Repeat("n", 200), Quantity: 1000,
			UnitPrice: &pb.Money{CurrencyCode: "USD", Units: MaxUnits, Nanos: 999_999_999}, Picture: strings.Repeat("p", 500)})
	}
	r.ShippingCost = &pb.Money{CurrencyCode: "USD", Units: MaxUnits, Nanos: 999_999_999}
	// 100 lines x 1000 x (1e9 + 0.999999999) + shipping (1e9 + 0.999999999)
	//   = 1e14 + 99999.9999 + 1e9 + 1.999999999 = (1e14 + 1e9 + 100000) units and 999,899,999 nanos
	r.Total = &pb.Money{CurrencyCode: "USD", Units: 100_000_000_000_000 + MaxUnits + 99_999 + 1, Nanos: 999_899_999}
	if _, err := ValidateNewOrder(r); err != nil {
		t.Fatalf("limits rejected: %v", err)
	}
}

func TestSumMoneyCarriesNanos(t *testing.T) {
	items := []Item{
		{Quantity: 3, UnitPrice: Money{"USD", 0, 400_000_000}},
		{Quantity: 1, UnitPrice: Money{"USD", 1, 999_999_999}},
	}
	got := sumMoney(items, Money{"USD", 0, 2})
	if got != (Money{"USD", 3, 200_000_001}) { // 1.2 + 1.999999999 + 0.000000002
		t.Errorf("sum = %+v", got)
	}
}

func TestFingerprint(t *testing.T) {
	a, _ := ValidateNewOrder(validRequest())
	b, _ := ValidateNewOrder(validRequest())
	if a.Fingerprint() != b.Fingerprint() {
		t.Error("same content, different fingerprint")
	}
	r := validRequest()
	r.Items[0].Quantity = 3
	r.Total = &pb.Money{CurrencyCode: "USD", Units: 70, Nanos: 220_000_000}
	c, err := ValidateNewOrder(r)
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint() == c.Fingerprint() {
		t.Error("different content, same fingerprint")
	}
}

func TestPage(t *testing.T) {
	cases := []struct {
		page, size    int32
		limit, offset int64
		bad           bool
	}{
		{0, 0, 10, 0, false},
		{1, 0, 10, 0, false},
		{3, 5, 5, 10, false},
		{1, 51, 50, 0, false},
		{2, 1000, 50, 50, false},
		{-1, 10, 0, 0, true},
		{1, -1, 0, 0, true},
		{1_000_001, 10, 0, 0, true},
		{1_000_000, 50, 50, 999_999 * 50, false},
	}
	for _, c := range cases {
		l, o, err := Page(c.page, c.size)
		if c.bad {
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Page(%d,%d) err = %v", c.page, c.size, err)
			}
			continue
		}
		if err != nil || l != c.limit || o != c.offset {
			t.Errorf("Page(%d,%d) = %d,%d,%v want %d,%d", c.page, c.size, l, o, err, c.limit, c.offset)
		}
	}
}

func TestValidateList(t *testing.T) {
	f, l, o, err := ValidateList(&pb.ListOrdersRequest{SessionId: " s ", Page: 2, PageSize: 5, PlacedOnly: true})
	if err != nil || f != (Filter{SessionID: "s", PlacedOnly: true}) || l != 5 || o != 5 {
		t.Errorf("session list: %+v %d %d %v", f, l, o, err)
	}
	if f, _, _, err = ValidateList(&pb.ListOrdersRequest{UserId: "u"}); err != nil || f.UserID != "u" {
		t.Errorf("user list: %+v %v", f, err)
	}
	for _, r := range []*pb.ListOrdersRequest{
		{},
		{SessionId: "s", UserId: "u"},
		{SessionId: strings.Repeat("s", 129)},
		{SessionId: "s", Page: -1},
	} {
		if _, _, _, err := ValidateList(r); !errors.Is(err, ErrInvalid) {
			t.Errorf("%v: err = %v", r, err)
		}
	}
}

func TestValidateConfirmFailGet(t *testing.T) {
	if _, _, _, err := ValidateConfirm(&pb.ConfirmOrderRequest{OrderId: "o", ShippingTrackingId: "t"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("missing txn: %v", err)
	}
	if _, _, _, err := ValidateConfirm(&pb.ConfirmOrderRequest{OrderId: "o", PaymentTransactionId: "x"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("missing tracking: %v", err)
	}
	if _, _, _, err := ValidateConfirm(&pb.ConfirmOrderRequest{OrderId: "o", ShippingTrackingId: strings.Repeat("t", 129), PaymentTransactionId: "x"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("long tracking: %v", err)
	}
	if _, _, err := ValidateFail(&pb.FailOrderRequest{OrderId: "o"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("missing reason: %v", err)
	}
	if _, _, err := ValidateFail(&pb.FailOrderRequest{OrderId: "o", Reason: strings.Repeat("r", 501)}); !errors.Is(err, ErrInvalid) {
		t.Errorf("long reason: %v", err)
	}
	if _, _, _, err := ValidateGet(&pb.GetOrderRequest{OrderId: "o", SessionId: strings.Repeat("s", 129)}); !errors.Is(err, ErrInvalid) {
		t.Errorf("long guard: %v", err)
	}
	if id, s, u, err := ValidateGet(&pb.GetOrderRequest{OrderId: "o"}); err != nil || id != "o" || s != "" || u != "" {
		t.Errorf("plain get: %v %q %q %v", id, s, u, err)
	}
}

func TestValidateClaim(t *testing.T) {
	s, u, err := ValidateClaim(&pb.ClaimOrdersRequest{SessionId: " sess ", UserId: "user"})
	if err != nil || s != "sess" || u != "user" {
		t.Errorf("valid claim: %q %q %v", s, u, err)
	}
	for _, r := range []*pb.ClaimOrdersRequest{nil, {}, {SessionId: "s"}, {UserId: "u"},
		{SessionId: strings.Repeat("s", 129), UserId: "u"}, {SessionId: "s", UserId: strings.Repeat("u", 129)}} {
		if _, _, err := ValidateClaim(r); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidateClaim(%v): %v", r, err)
		}
	}
}
