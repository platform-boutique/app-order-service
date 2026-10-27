package orders

import (
	"context"
	"sort"
	"sync"
	"time"

	pb "order-service/proto"
)

// validRequest is a correct CreateOrder request: 2 x 19.99 + 1 x 5.50 + shipping 4.75 = 50.23 USD.
func validRequest() *pb.CreateOrderRequest {
	usd := func(units int64, nanos int32) *pb.Money {
		return &pb.Money{CurrencyCode: "USD", Units: units, Nanos: nanos}
	}
	return &pb.CreateOrderRequest{
		OrderId:         "ord-1",
		SessionId:       "sess-1",
		Email:           "ada@example.com",
		ShippingAddress: &pb.Address{StreetAddress: "1 Main St", City: "London", State: "", Country: "UK", ZipCode: 1234},
		Items: []*pb.OrderItem{
			{ProductId: "tee", Name: "Tee", Quantity: 2, UnitPrice: usd(19, 990_000_000), Picture: "/img/tee.jpg"},
			{ProductId: "mug", Name: "Mug", Quantity: 1, UnitPrice: usd(5, 500_000_000)},
		},
		ShippingCost: usd(4, 750_000_000),
		Total:        usd(50, 230_000_000),
	}
}

// memStore is an in-memory Store with the same semantics as PGStore (it shares ApplyConfirm/ApplyFail).
type memStore struct {
	mu     sync.Mutex
	orders map[string]Order
	err    error // returned by every call when set
}

func newMemStore() *memStore { return &memStore{orders: map[string]Order{}} }

func (m *memStore) Create(_ context.Context, n NewOrder, now time.Time) (Order, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return Order{}, false, m.err
	}
	o := Order{NewOrder: n, State: StatePending, Fingerprint: n.Fingerprint(), CreatedAt: now}
	if old, ok := m.orders[n.ID]; ok {
		if old.Fingerprint != o.Fingerprint {
			return Order{}, false, ErrConflict
		}
		return old, false, nil
	}
	m.orders[n.ID] = o
	return o, true, nil
}

func (m *memStore) Get(_ context.Context, id string) (Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return Order{}, m.err
	}
	o, ok := m.orders[id]
	if !ok {
		return Order{}, ErrNotFound
	}
	return o, nil
}

func (m *memStore) transition(id string, f func(Order) (Order, bool, error)) (Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return Order{}, m.err
	}
	o, ok := m.orders[id]
	if !ok {
		return Order{}, ErrNotFound
	}
	out, changed, err := f(o)
	if err != nil {
		return Order{}, err
	}
	if changed {
		m.orders[id] = out
	}
	return out, nil
}

func (m *memStore) Confirm(_ context.Context, id, tracking, txn string, now time.Time) (Order, error) {
	return m.transition(id, func(o Order) (Order, bool, error) { return ApplyConfirm(o, tracking, txn, now) })
}

func (m *memStore) Fail(_ context.Context, id, reason string, now time.Time) (Order, error) {
	return m.transition(id, func(o Order) (Order, bool, error) { return ApplyFail(o, reason, now) })
}

func (m *memStore) List(_ context.Context, f Filter, limit, offset int64) ([]Order, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, 0, m.err
	}
	var all []Order
	for _, o := range m.orders {
		mine := (f.SessionID != "" && o.SessionID == f.SessionID) || (f.UserID != "" && o.UserID == f.UserID)
		if mine && (!f.PlacedOnly || o.State == StatePlaced) {
			all = append(all, o)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return all[i].ID > all[j].ID
	})
	total := int64(len(all))
	if offset >= total {
		return []Order{}, total, nil
	}
	return all[offset:min(offset+limit, total)], total, nil
}

func (m *memStore) Ping(context.Context) error { return m.err }
