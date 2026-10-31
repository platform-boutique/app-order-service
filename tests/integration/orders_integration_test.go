//go:build integration

// Integration tests against a real Postgres. Run with:
//
//	TEST_DATABASE_URL=postgres://order:order@localhost:55433/postgres?sslmode=disable \
//	  go test -race -tags integration ./tests/integration/
//
// Every test creates (and drops) its own database, so they need a user that may CREATE DATABASE.
package integration

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"order-service/internal/orders"
	pb "order-service/proto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// freshDB creates an empty database and returns its DSN.
func freshDB(t *testing.T) string {
	t.Helper()
	admin := os.Getenv("TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	name := fmt.Sprintf("orders_test_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// clock is a settable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// setup migrates a fresh database and returns the service on it.
func setup(t *testing.T) (*orders.Service, *pgxpool.Pool, *clock) {
	t.Helper()
	p := newPool(t, freshDB(t))
	if _, err := orders.Setup(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	l := logrus.New()
	l.Out = io.Discard
	clk := &clock{t: time.Date(2026, 10, 27, 12, 0, 0, 123_456_789, time.UTC)} // sub-microsecond part on purpose
	svc := orders.NewService(orders.NewPGStore(p), l, orders.DefaultStages, clk.Now)
	svc.SetReady(true)
	return svc, p, clk
}

func usd(units int64, nanos int32) *pb.Money {
	return &pb.Money{CurrencyCode: "USD", Units: units, Nanos: nanos}
}

// request: 2 x 19.99 + 1 x 5.50 + shipping 4.75 = 50.23 USD
func request(id, session, user string) *pb.CreateOrderRequest {
	return &pb.CreateOrderRequest{
		OrderId: id, SessionId: session, UserId: user, Email: "ada@example.com",
		ShippingAddress: &pb.Address{StreetAddress: "1 Main St ☕", City: "Zürich", State: "ZH", Country: "CH", ZipCode: 8001},
		Items: []*pb.OrderItem{
			{ProductId: "tee", Name: "Tee", Quantity: 2, UnitPrice: usd(19, 990_000_000), Picture: "/img/tee.jpg"},
			{ProductId: "mug", Name: "Mug", Quantity: 1, UnitPrice: usd(5, 500_000_000)},
			{ProductId: "tee", Name: "Tee (again)", Quantity: 1, UnitPrice: usd(0, 10_000_000)}, // same product twice, order kept
		},
		ShippingCost: usd(4, 750_000_000),
		Total:        usd(50, 240_000_000),
	}
}

func confirm(id string) *pb.ConfirmOrderRequest {
	return &pb.ConfirmOrderRequest{OrderId: id, ShippingTrackingId: "TRK-1", PaymentTransactionId: "TXN-1"}
}

func count(t *testing.T, p *pgxpool.Pool, table string) int64 {
	t.Helper()
	var n int64
	if err := p.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMigrationsTwice(t *testing.T) {
	p := newPool(t, freshDB(t))
	ctx := context.Background()
	applied, err := orders.Setup(ctx, p)
	if err != nil || applied != 1 {
		t.Fatalf("first run: applied %d, %v (want 1)", applied, err)
	}
	applied, err = orders.Setup(ctx, p)
	if err != nil || applied != 0 {
		t.Fatalf("second run: applied %d, %v (want 0)", applied, err)
	}
	var idx int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname IN ('orders_session_created_idx', 'orders_user_created_idx')`).Scan(&idx); err != nil || idx != 2 {
		t.Errorf("indexes: %d, %v", idx, err)
	}
}

func TestConcurrentStarts(t *testing.T) {
	dsn := freshDB(t)
	const replicas = 6
	var wg sync.WaitGroup
	var applied atomic.Int64
	errs := make(chan error, replicas)
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := pgxpool.New(context.Background(), dsn) // one pool per "replica"
			if err != nil {
				errs <- err
				return
			}
			defer p.Close()
			n, err := orders.Setup(context.Background(), p)
			if err != nil {
				errs <- err
				return
			}
			applied.Add(int64(n))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("replica failed: %v", err)
	}
	if applied.Load() != 1 {
		t.Errorf("over %d replicas the migration was applied %d times, want 1", replicas, applied.Load())
	}
}

func TestLifecycle(t *testing.T) {
	svc, p, clk := setup(t)
	ctx := context.Background()

	created, err := svc.CreateOrder(ctx, request("ord-1", "sess-1", ""))
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("status %v", created.Status)
	}
	got, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1", SessionId: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	// everything survives the round trip, items in their original order
	if got.Email != "ada@example.com" || got.ShippingAddress.StreetAddress != "1 Main St ☕" || got.ShippingAddress.City != "Zürich" ||
		got.ShippingAddress.ZipCode != 8001 || got.Total.Units != 50 || got.Total.Nanos != 240_000_000 || got.ShippingCost.Nanos != 750_000_000 {
		t.Errorf("round trip: %v", got)
	}
	if len(got.Items) != 3 || got.Items[0].Name != "Tee" || got.Items[0].Quantity != 2 || got.Items[0].UnitPrice.Nanos != 990_000_000 ||
		got.Items[0].Picture != "/img/tee.jpg" || got.Items[1].ProductId != "mug" || got.Items[2].Name != "Tee (again)" || got.Items[2].UnitPrice.CurrencyCode != "USD" {
		t.Errorf("items: %v", got.Items)
	}
	// the stored timestamp equals the one the create call returned (microsecond precision)
	if !got.CreatedAt.AsTime().Equal(created.CreatedAt.AsTime()) {
		t.Errorf("created_at %v vs %v", got.CreatedAt.AsTime(), created.CreatedAt.AsTime())
	}

	// idempotent create, conflicting create
	if _, err := svc.CreateOrder(ctx, request("ord-1", "sess-1", "")); err != nil {
		t.Errorf("retry: %v", err)
	}
	other := request("ord-1", "sess-1", "")
	other.Items[1].Quantity = 2
	other.Total = usd(55, 740_000_000)
	if _, err := svc.CreateOrder(ctx, other); status.Code(err) != codes.AlreadyExists {
		t.Errorf("same id, other content: %v", err)
	}
	if n, ni := count(t, p, "orders"), count(t, p, "order_items"); n != 1 || ni != 3 {
		t.Errorf("rows: %d orders, %d items (want 1, 3)", n, ni)
	}

	// confirm, then the status moves with the injected clock
	clk.Advance(10 * time.Second)
	placed, err := svc.ConfirmOrder(ctx, confirm("ord-1"))
	if err != nil || placed.Status != pb.OrderStatus_ORDER_STATUS_PLACED || placed.ShippingTrackingId != "TRK-1" {
		t.Fatalf("confirm: %v %v", placed, err)
	}
	if _, err := svc.ConfirmOrder(ctx, confirm("ord-1")); err != nil {
		t.Errorf("confirm retry: %v", err)
	}
	wrong := confirm("ord-1")
	wrong.PaymentTransactionId = "OTHER"
	if _, err := svc.ConfirmOrder(ctx, wrong); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("confirm with other references: %v", err)
	}
	if _, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-1", Reason: "late"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("fail a placed order: %v", err)
	}
	for _, s := range []struct {
		after time.Duration
		want  pb.OrderStatus
	}{
		{0, pb.OrderStatus_ORDER_STATUS_PLACED},
		{time.Minute, pb.OrderStatus_ORDER_STATUS_PROCESSING},
		{3 * time.Minute, pb.OrderStatus_ORDER_STATUS_SHIPPED},
		{6 * time.Minute, pb.OrderStatus_ORDER_STATUS_DELIVERED},
	} {
		clk.t = placed.PlacedAt.AsTime().Add(s.after)
		o, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"})
		if err != nil || o.Status != s.want {
			t.Errorf("%v after placement: %v %v, want %v", s.after, o.GetStatus(), err, s.want)
		}
	}
	if _, err := svc.ConfirmOrder(ctx, confirm("missing")); status.Code(err) != codes.NotFound {
		t.Errorf("confirm unknown: %v", err)
	}
}

func TestFailPath(t *testing.T) {
	svc, _, clk := setup(t)
	ctx := context.Background()
	if _, err := svc.CreateOrder(ctx, request("ord-f", "sess-1", "")); err != nil {
		t.Fatal(err)
	}
	o, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-f", Reason: "card declined"})
	if err != nil || o.Status != pb.OrderStatus_ORDER_STATUS_FAILED || o.FailureReason != "card declined" || o.FailedAt == nil {
		t.Fatalf("fail: %v %v", o, err)
	}
	if again, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-f", Reason: "something else"}); err != nil || again.FailureReason != "card declined" {
		t.Errorf("fail retry: %v %v", again, err)
	}
	if _, err := svc.ConfirmOrder(ctx, confirm("ord-f")); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("confirm failed order: %v", err)
	}
	clk.Advance(100 * time.Hour)
	got, _ := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-f"})
	if got.Status != pb.OrderStatus_ORDER_STATUS_FAILED || got.PlacedAt != nil {
		t.Errorf("failed order advanced: %v", got)
	}
}

func TestConcurrentCreateSameID(t *testing.T) {
	svc, p, _ := setup(t)
	const n = 12
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.CreateOrder(context.Background(), request("ord-race", "sess-1", ""))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent identical create: %v", err)
		}
	}
	if c, ci := count(t, p, "orders"), count(t, p, "order_items"); c != 1 || ci != 3 {
		t.Errorf("rows: %d orders, %d items (want 1, 3)", c, ci)
	}
}

// Confirm and Fail race on the same order: exactly one of them wins, the other gets FAILED_PRECONDITION.
func TestConcurrentConfirmAndFail(t *testing.T) {
	svc, _, _ := setup(t)
	ctx := context.Background()
	for round := 0; round < 10; round++ {
		id := fmt.Sprintf("ord-r%d", round)
		if _, err := svc.CreateOrder(ctx, request(id, "sess-1", "")); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var confirmErr, failErr error
		wg.Add(2)
		go func() { defer wg.Done(); _, confirmErr = svc.ConfirmOrder(ctx, confirm(id)) }()
		go func() {
			defer wg.Done()
			_, failErr = svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: id, Reason: "x"})
		}()
		wg.Wait()
		if (confirmErr == nil) == (failErr == nil) {
			t.Fatalf("round %d: confirm err %v, fail err %v: exactly one must succeed", round, confirmErr, failErr)
		}
		final, _ := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: id})
		isPlaced := final.Status != pb.OrderStatus_ORDER_STATUS_FAILED
		if isPlaced != (confirmErr == nil) {
			t.Errorf("round %d: stored state %v does not match the winner", round, final.Status)
		}
	}
}

func TestListAndGuards(t *testing.T) {
	svc, _, clk := setup(t)
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		if _, err := svc.CreateOrder(ctx, request(fmt.Sprintf("a%d", i), "sess-a", "")); err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Second)
	}
	// same created_at for t1, t2 and b1: the id (descending) breaks the tie
	for _, id := range []string{"t1", "t2"} {
		if _, err := svc.CreateOrder(ctx, request(id, "sess-t", "user-7")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.CreateOrder(ctx, request("b1", "sess-b", "user-7")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ConfirmOrder(ctx, confirm("a1")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "a2", Reason: "declined"}); err != nil {
		t.Fatal(err)
	}

	ids := func(r *pb.ListOrdersResponse) string {
		s := ""
		for _, o := range r.Orders {
			s += o.OrderId + " "
		}
		return s
	}
	r, err := svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", Page: 1, PageSize: 2})
	if err != nil || r.Total != 5 || ids(r) != "a5 a4 " || len(r.Orders[0].Items) != 3 {
		t.Errorf("page 1: %q total %d, %v", ids(r), r.GetTotal(), err)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", Page: 3, PageSize: 2})
	if ids(r) != "a1 " || r.Total != 5 {
		t.Errorf("page 3: %q", ids(r))
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", Page: 4, PageSize: 2})
	if len(r.Orders) != 0 || r.Total != 5 {
		t.Errorf("past the end: %q total %d", ids(r), r.Total)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", PlacedOnly: true})
	if ids(r) != "a1 " || r.Total != 1 {
		t.Errorf("placed_only: %q total %d", ids(r), r.Total)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{UserId: "user-7"})
	if r.Total != 3 || ids(r) != "t2 t1 b1 " {
		t.Errorf("by user: %q total %d", ids(r), r.Total)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-b"})
	if ids(r) != "b1 " {
		t.Errorf("sessions leak into each other: %q", ids(r))
	}

	// GetOrder guard
	if _, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "a1", SessionId: "sess-b"}); status.Code(err) != codes.NotFound {
		t.Errorf("foreign session: %v", err)
	}
	if _, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "t1", UserId: "user-7"}); err != nil {
		t.Errorf("own user: %v", err)
	}
	if _, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "a1", UserId: "user-7"}); status.Code(err) != codes.NotFound {
		t.Errorf("order without user, user guard: %v", err)
	}
}

// The database constraints back up the validation.
func TestConstraints(t *testing.T) {
	svc, p, _ := setup(t)
	ctx := context.Background()
	if _, err := svc.CreateOrder(ctx, request("ord-c", "sess-1", "")); err != nil {
		t.Fatal(err)
	}
	for name, sql := range map[string]string{
		"bad state":       `UPDATE orders SET state = 'DELIVERED' WHERE id = 'ord-c'`,
		"placed w/o refs": `UPDATE orders SET state = 'PLACED', confirmed_at = now() WHERE id = 'ord-c'`,
		"failed w/o time": `UPDATE orders SET state = 'FAILED' WHERE id = 'ord-c'`,
		"negative total":  `UPDATE orders SET total_units = -1 WHERE id = 'ord-c'`,
		"nanos too big":   `UPDATE orders SET total_nanos = 1000000000 WHERE id = 'ord-c'`,
		"bad currency":    `UPDATE orders SET currency = 'usd' WHERE id = 'ord-c'`,
		"quantity 0":      `UPDATE order_items SET quantity = 0 WHERE order_id = 'ord-c'`,
		"orphan item":     `INSERT INTO order_items (order_id, position, product_id, name, quantity, unit_units, unit_nanos) VALUES ('nope', 0, 'p', 'n', 1, 1, 0)`,
	} {
		if _, err := p.Exec(ctx, sql); err == nil {
			t.Errorf("%s: accepted by the database", name)
		}
	}
	// invalid input never reaches the database
	bad := request("ord-bad", "sess-1", "")
	bad.Items[0].Quantity = 0
	if _, err := svc.CreateOrder(ctx, bad); status.Code(err) != codes.InvalidArgument {
		t.Errorf("quantity 0: %v", err)
	}
	if n := count(t, p, "orders"); n != 1 {
		t.Errorf("orders %d, want 1", n)
	}
}

func TestUnreachableDatabase(t *testing.T) {
	p := newPool(t, "postgres://order:order@127.0.0.1:1/none?sslmode=disable&connect_timeout=2")
	l := logrus.New()
	l.Out = io.Discard
	svc := orders.NewService(orders.NewPGStore(p), l, orders.DefaultStages, nil)
	svc.SetReady(true)
	_, err := svc.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: "x"})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("unreachable DB: %v, want Unavailable", err)
	}
	if _, err := svc.CreateOrder(context.Background(), request("x", "s", "")); status.Code(err) != codes.Unavailable {
		t.Errorf("create on unreachable DB: %v", err)
	}
	if _, err := orders.Setup(context.Background(), p); err == nil {
		t.Error("Setup succeeded without a database")
	}
}

func claim(t *testing.T, svc *orders.Service, session, user string) int32 {
	t.Helper()
	r, err := svc.ClaimOrders(context.Background(), &pb.ClaimOrdersRequest{SessionId: session, UserId: user})
	if err != nil {
		t.Fatalf("ClaimOrders(%s, %s): %v", session, user, err)
	}
	return r.Claimed
}

func TestClaimOrders(t *testing.T) {
	svc, p, _ := setup(t)
	ctx := context.Background()
	// sess-1: a PLACED, a PENDING and a FAILED order, all anonymous; sess-2 has one anonymous order;
	// sess-3 has one that already belongs to user-x
	for _, id := range []string{"c-placed", "c-pending", "c-failed"} {
		if _, err := svc.CreateOrder(ctx, request(id, "sess-1", "")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.ConfirmOrder(ctx, confirm("c-placed")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "c-failed", Reason: "declined"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateOrder(ctx, request("d-other", "sess-2", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateOrder(ctx, request("e-owned", "sess-3", "user-x")); err != nil {
		t.Fatal(err)
	}

	if n := claim(t, svc, "sess-1", "user-1"); n != 3 {
		t.Fatalf("first claim = %d, want 3 (PLACED, PENDING and FAILED)", n)
	}
	if n := claim(t, svc, "sess-1", "user-1"); n != 0 {
		t.Errorf("same user again = %d, want 0", n)
	}
	if n := claim(t, svc, "sess-1", "user-2"); n != 0 {
		t.Errorf("another user on the claimed session = %d, want 0", n)
	}
	if n := claim(t, svc, "sess-3", "user-2"); n != 0 {
		t.Errorf("claim over an order that has a user = %d, want 0", n)
	}
	if n := claim(t, svc, "no-such-session", "user-2"); n != 0 {
		t.Errorf("unknown session = %d, want 0", n)
	}

	// only the matching session was claimed, and the owner stayed where it was
	var owner string
	for id, want := range map[string]string{"c-placed": "user-1", "c-pending": "user-1", "c-failed": "user-1", "d-other": "", "e-owned": "user-x"} {
		if err := p.QueryRow(ctx, `SELECT user_id FROM orders WHERE id = $1`, id).Scan(&owner); err != nil || owner != want {
			t.Errorf("%s: user_id %q, %v (want %q)", id, owner, err, want)
		}
	}

	// ListOrders by user returns them (all states), the placed filter keeps the PLACED one
	r, err := svc.ListOrders(ctx, &pb.ListOrdersRequest{UserId: "user-1"})
	if err != nil || r.Total != 3 || len(r.Orders) != 3 {
		t.Fatalf("list by user: %v, %v", r, err)
	}
	r, err = svc.ListOrders(ctx, &pb.ListOrdersRequest{UserId: "user-1", PlacedOnly: true})
	if err != nil || r.Total != 1 || r.Orders[0].OrderId != "c-placed" || r.Orders[0].UserId != "user-1" {
		t.Fatalf("list by user, placed only: %v, %v", r, err)
	}
	if r, err = svc.ListOrders(ctx, &pb.ListOrdersRequest{UserId: "user-2"}); err != nil || r.Total != 0 {
		t.Errorf("user-2 sees %v, %v", r, err)
	}
	// GetOrder with the user guard: the owner gets it, anyone else NOT_FOUND; the session id is kept
	o, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "c-placed", UserId: "user-1"})
	if err != nil || o.UserId != "user-1" || o.SessionId != "sess-1" {
		t.Errorf("owner get: %v, %v", o, err)
	}
	if _, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "c-placed", UserId: "user-2"}); status.Code(err) != codes.NotFound {
		t.Errorf("other user get: %v", err)
	}
	// the claimed order is still readable with its session guard
	if _, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "c-placed", SessionId: "sess-1"}); err != nil {
		t.Errorf("session get after claim: %v", err)
	}
}

// Two users claim the same sessions at the same moment: per session exactly one wins every order.
func TestConcurrentClaims(t *testing.T) {
	svc, p, _ := setup(t)
	ctx := context.Background()
	const sessions, perSession = 12, 4
	for s := 0; s < sessions; s++ {
		for i := 0; i < perSession; i++ {
			if _, err := svc.CreateOrder(ctx, request(fmt.Sprintf("o-%d-%d", s, i), fmt.Sprintf("sess-%d", s), "")); err != nil {
				t.Fatal(err)
			}
		}
	}
	type result struct{ a, b int32 }
	res := make([]result, sessions)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for s := 0; s < sessions; s++ {
		for u, user := range []string{"user-a", "user-b"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				r, err := svc.ClaimOrders(ctx, &pb.ClaimOrdersRequest{SessionId: fmt.Sprintf("sess-%d", s), UserId: user})
				if err != nil {
					t.Error(err)
					return
				}
				if u == 0 {
					res[s].a = r.Claimed
				} else {
					res[s].b = r.Claimed
				}
			}()
		}
	}
	close(start)
	wg.Wait()
	for s, r := range res {
		if r.a+r.b != perSession || (r.a != 0 && r.b != 0) {
			t.Errorf("session %d: user-a claimed %d, user-b claimed %d (want one winner with %d)", s, r.a, r.b, perSession)
		}
		// all orders of one session ended with the same single owner
		var owners int
		if err := p.QueryRow(ctx, `SELECT count(DISTINCT user_id) FROM orders WHERE session_id = $1`, fmt.Sprintf("sess-%d", s)).Scan(&owners); err != nil || owners != 1 {
			t.Errorf("session %d has %d owners, %v", s, owners, err)
		}
	}
	var unowned int64
	if err := p.QueryRow(ctx, `SELECT count(*) FROM orders WHERE user_id = ''`).Scan(&unowned); err != nil || unowned != 0 {
		t.Errorf("%d orders left unowned, %v", unowned, err)
	}
}
