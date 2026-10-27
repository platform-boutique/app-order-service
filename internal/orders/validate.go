package orders

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"unicode/utf8"

	pb "order-service/proto"
)

// Limits of the API contract (see proto/orders.proto).
const (
	DefaultPageSize = 10
	MaxPageSize     = 50
	maxPage         = 1_000_000 // keeps the OFFSET far below any overflow

	MaxOrderIDLen   = 64
	MaxSessionIDLen = 128
	MaxUserIDLen    = 128
	MaxEmailLen     = 254
	MaxItems        = 100
	MaxQuantity     = 1000
	MaxProductIDLen = 128
	MaxNameLen      = 200
	MaxPictureLen   = 500
	MaxStreetLen    = 200
	MaxCityLen      = 100
	MaxStateLen     = 100
	MaxCountryLen   = 100
	MaxReasonLen    = 500
	MaxRefIDLen     = 128 // tracking id, payment transaction id

	// MaxUnits bounds one money amount (a unit price or shipping cost), so that
	// 100 lines x 1000 pieces can never overflow an int64.
	MaxUnits = 1_000_000_000

	nanosPerUnit = 1_000_000_000
)

// ErrInvalid marks a request that breaks the contract; the gRPC layer maps it to INVALID_ARGUMENT.
var ErrInvalid = errors.New("invalid argument")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Money is a non-negative amount in one currency.
type Money struct {
	Currency string
	Units    int64
	Nanos    int32
}

// Item is one order line.
type Item struct {
	ProductID string
	Name      string
	Quantity  int32
	UnitPrice Money
	Picture   string
}

// Address is the shipping address (same shape as hipstershop.Address).
type Address struct {
	Street  string
	City    string
	State   string
	Country string
	Zip     int32
}

// NewOrder is a validated, normalised order that has not been stored yet.
type NewOrder struct {
	ID           string
	SessionID    string
	UserID       string
	Email        string
	Address      Address
	Items        []Item
	ShippingCost Money
	Total        Money
}

// Fingerprint is a hash of everything the client sent; CreateOrder compares it to tell a harmless
// retry (same content) from a different order reusing an id.
func (n NewOrder) Fingerprint() string {
	b, _ := json.Marshal(n) // a struct of strings and numbers: cannot fail
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// text trims s and checks length (in characters), UTF-8 and NUL bytes.
func text(field, s string, minLen, maxLen int) (string, error) {
	s = strings.TrimSpace(s)
	if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
		return "", invalid("%s must be valid UTF-8 text without NUL characters", field)
	}
	if n := utf8.RuneCountInString(s); n < minLen || n > maxLen {
		if minLen == 0 {
			return "", invalid("%s must be at most %d characters, got %d", field, maxLen, n)
		}
		return "", invalid("%s must be %d-%d characters, got %d", field, minLen, maxLen, n)
	}
	return s, nil
}

// ValidateOrderID checks the client-supplied id: 1-64 characters of [A-Za-z0-9._-], not starting with ._-.
func ValidateOrderID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", invalid("order_id is required")
	}
	if len(id) > MaxOrderIDLen {
		return "", invalid("order_id must be at most %d characters", MaxOrderIDLen)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && (i == 0 || (c != '.' && c != '_' && c != '-')) {
			return "", invalid("order_id may only contain letters, digits, '.', '_' and '-' and must start with a letter or digit")
		}
	}
	return id, nil
}

// ValidateMoney checks one amount: 3 upper-case letters currency, 0 <= units <= MaxUnits,
// 0 <= nanos < 1e9. A zero amount is only accepted when allowZero.
func ValidateMoney(field string, m *pb.Money, allowZero bool) (Money, error) {
	if m == nil {
		return Money{}, invalid("%s is required", field)
	}
	cur := m.GetCurrencyCode()
	if len(cur) != 3 || strings.Trim(cur, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") != "" {
		return Money{}, invalid("%s.currency_code must be three upper-case letters", field)
	}
	if m.GetUnits() < 0 || m.GetUnits() > MaxUnits {
		return Money{}, invalid("%s.units must be 0-%d, got %d", field, int64(MaxUnits), m.GetUnits())
	}
	if m.GetNanos() < 0 || m.GetNanos() >= nanosPerUnit {
		return Money{}, invalid("%s.nanos must be 0-999999999, got %d", field, m.GetNanos())
	}
	if !allowZero && m.GetUnits() == 0 && m.GetNanos() == 0 {
		return Money{}, invalid("%s must be greater than zero", field)
	}
	return Money{Currency: cur, Units: m.GetUnits(), Nanos: m.GetNanos()}, nil
}

// sumMoney adds quantity x unit price over the lines (exact, in units and nanos) and the shipping cost.
func sumMoney(items []Item, shipping Money) Money {
	var units, nanos int64
	for _, it := range items {
		q := int64(it.Quantity)
		units += it.UnitPrice.Units * q
		nanos += int64(it.UnitPrice.Nanos) * q
	}
	units += shipping.Units
	nanos += int64(shipping.Nanos)
	units += nanos / nanosPerUnit
	nanos %= nanosPerUnit
	return Money{Currency: shipping.Currency, Units: units, Nanos: int32(nanos)} //#nosec G115 -- nanos < 1e9
}

// ValidateNewOrder checks every limit of CreateOrder and returns the normalised order.
func ValidateNewOrder(req *pb.CreateOrderRequest) (NewOrder, error) {
	var n NewOrder
	var err error
	if n.ID, err = ValidateOrderID(req.GetOrderId()); err != nil {
		return NewOrder{}, err
	}
	if n.SessionID, err = text("session_id", req.GetSessionId(), 1, MaxSessionIDLen); err != nil {
		return NewOrder{}, err
	}
	if n.UserID, err = text("user_id", req.GetUserId(), 0, MaxUserIDLen); err != nil {
		return NewOrder{}, err
	}
	if n.Email, err = validateEmail(req.GetEmail()); err != nil {
		return NewOrder{}, err
	}
	if n.Address, err = validateAddress(req.GetShippingAddress()); err != nil {
		return NewOrder{}, err
	}

	items := req.GetItems()
	if len(items) < 1 || len(items) > MaxItems {
		return NewOrder{}, invalid("items must contain 1-%d lines, got %d", MaxItems, len(items))
	}
	n.Items = make([]Item, 0, len(items))
	for i, it := range items {
		item, err := validateItem(i, it)
		if err != nil {
			return NewOrder{}, err
		}
		n.Items = append(n.Items, item)
	}
	cur := n.Items[0].UnitPrice.Currency

	total, err := ValidateMoney("total", req.GetTotal(), false)
	if err != nil {
		return NewOrder{}, err
	}
	n.Total = total
	if req.GetShippingCost() == nil {
		n.ShippingCost = Money{Currency: total.Currency}
	} else if n.ShippingCost, err = ValidateMoney("shipping_cost", req.GetShippingCost(), true); err != nil {
		return NewOrder{}, err
	}
	for i, it := range n.Items {
		if it.UnitPrice.Currency != cur {
			return NewOrder{}, invalid("items[%d].unit_price uses %s, items[0] uses %s: one currency per order", i, it.UnitPrice.Currency, cur)
		}
	}
	if n.ShippingCost.Currency != cur || n.Total.Currency != cur {
		return NewOrder{}, invalid("shipping_cost and total must use the currency of the items (%s)", cur)
	}
	if want := sumMoney(n.Items, n.ShippingCost); want != n.Total {
		return NewOrder{}, invalid("total %d.%09d does not equal items plus shipping %d.%09d", n.Total.Units, n.Total.Nanos, want.Units, want.Nanos)
	}
	return n, nil
}

func validateEmail(raw string) (string, error) {
	s, err := text("email", raw, 3, MaxEmailLen)
	if err != nil {
		return "", err
	}
	a, err := mail.ParseAddress(s)
	if err != nil || a.Address != s || a.Name != "" {
		return "", invalid("email must be a plain address like name@example.com")
	}
	return s, nil
}

func validateAddress(a *pb.Address) (Address, error) {
	if a == nil {
		return Address{}, invalid("shipping_address is required")
	}
	var out Address
	var err error
	if out.Street, err = text("shipping_address.street_address", a.GetStreetAddress(), 1, MaxStreetLen); err != nil {
		return Address{}, err
	}
	if out.City, err = text("shipping_address.city", a.GetCity(), 1, MaxCityLen); err != nil {
		return Address{}, err
	}
	if out.State, err = text("shipping_address.state", a.GetState(), 0, MaxStateLen); err != nil {
		return Address{}, err
	}
	if out.Country, err = text("shipping_address.country", a.GetCountry(), 1, MaxCountryLen); err != nil {
		return Address{}, err
	}
	if a.GetZipCode() < 0 {
		return Address{}, invalid("shipping_address.zip_code must be >= 0, got %d", a.GetZipCode())
	}
	out.Zip = a.GetZipCode()
	return out, nil
}

func validateItem(i int, it *pb.OrderItem) (Item, error) {
	f := func(name string) string { return fmt.Sprintf("items[%d].%s", i, name) }
	if it == nil {
		return Item{}, invalid("items[%d] is empty", i)
	}
	var out Item
	var err error
	if out.ProductID, err = text(f("product_id"), it.GetProductId(), 1, MaxProductIDLen); err != nil {
		return Item{}, err
	}
	if out.Name, err = text(f("name"), it.GetName(), 1, MaxNameLen); err != nil {
		return Item{}, err
	}
	if q := it.GetQuantity(); q < 1 || q > MaxQuantity {
		return Item{}, invalid("%s must be 1-%d, got %d", f("quantity"), MaxQuantity, q)
	}
	out.Quantity = it.GetQuantity()
	if out.UnitPrice, err = ValidateMoney(f("unit_price"), it.GetUnitPrice(), false); err != nil {
		return Item{}, err
	}
	if out.Picture, err = text(f("picture"), it.GetPicture(), 0, MaxPictureLen); err != nil {
		return Item{}, err
	}
	return out, nil
}

// ValidateConfirm checks the arguments of ConfirmOrder.
func ValidateConfirm(req *pb.ConfirmOrderRequest) (id, tracking, txn string, err error) {
	if id, err = ValidateOrderID(req.GetOrderId()); err != nil {
		return
	}
	if tracking, err = text("shipping_tracking_id", req.GetShippingTrackingId(), 1, MaxRefIDLen); err != nil {
		return
	}
	txn, err = text("payment_transaction_id", req.GetPaymentTransactionId(), 1, MaxRefIDLen)
	return
}

// ValidateFail checks the arguments of FailOrder.
func ValidateFail(req *pb.FailOrderRequest) (id, reason string, err error) {
	if id, err = ValidateOrderID(req.GetOrderId()); err != nil {
		return
	}
	reason, err = text("reason", req.GetReason(), 1, MaxReasonLen)
	return
}

// ValidateGet checks the arguments of GetOrder; the guards are optional.
func ValidateGet(req *pb.GetOrderRequest) (id, sessionGuard, userGuard string, err error) {
	if id, err = ValidateOrderID(req.GetOrderId()); err != nil {
		return
	}
	if sessionGuard, err = text("session_id", req.GetSessionId(), 0, MaxSessionIDLen); err != nil {
		return
	}
	userGuard, err = text("user_id", req.GetUserId(), 0, MaxUserIDLen)
	return
}

// Filter selects the orders of ListOrders.
type Filter struct {
	SessionID  string
	UserID     string
	PlacedOnly bool
}

// ValidateList checks ListOrders: exactly one owner, bounded paging. Returns the filter, LIMIT and OFFSET.
func ValidateList(req *pb.ListOrdersRequest) (f Filter, limit, offset int64, err error) {
	if f.SessionID, err = text("session_id", req.GetSessionId(), 0, MaxSessionIDLen); err != nil {
		return
	}
	if f.UserID, err = text("user_id", req.GetUserId(), 0, MaxUserIDLen); err != nil {
		return
	}
	if (f.SessionID == "") == (f.UserID == "") {
		err = invalid("exactly one of session_id and user_id must be set")
		return
	}
	f.PlacedOnly = req.GetPlacedOnly()
	limit, offset, err = Page(req.GetPage(), req.GetPageSize())
	return
}

// Page turns the 1-based page and page size of ListOrders into LIMIT/OFFSET.
// page 0 means 1, page_size 0 means DefaultPageSize, sizes above MaxPageSize are capped;
// negative values (and absurdly large pages) are invalid.
func Page(page, pageSize int32) (limit, offset int64, err error) {
	if page < 0 {
		return 0, 0, invalid("page must be >= 1, got %d", page)
	}
	if page > maxPage {
		return 0, 0, invalid("page must be <= %d, got %d", maxPage, page)
	}
	if pageSize < 0 {
		return 0, 0, invalid("page_size must be >= 0, got %d", pageSize)
	}
	if page == 0 {
		page = 1
	}
	switch {
	case pageSize == 0:
		pageSize = DefaultPageSize
	case pageSize > MaxPageSize:
		pageSize = MaxPageSize
	}
	return int64(pageSize), int64(page-1) * int64(pageSize), nil
}
