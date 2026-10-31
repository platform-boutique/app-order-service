package orders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "order-service/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeClock is a settable clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestService(t *testing.T) (*Service, *memStore, *fakeClock) {
	t.Helper()
	l := logrus.New()
	l.Out = io.Discard
	st := newMemStore()
	clk := &fakeClock{t: t0}
	svc := NewService(st, l, DefaultStages, clk.Now)
	svc.SetReady(true)
	return svc, st, clk
}

func wantCode(t *testing.T, err error, c codes.Code) {
	t.Helper()
	if status.Code(err) != c {
		t.Fatalf("code = %v (%v), want %v", status.Code(err), err, c)
	}
}

func confirmReq(id string) *pb.ConfirmOrderRequest {
	return &pb.ConfirmOrderRequest{OrderId: id, ShippingTrackingId: "TRK-1", PaymentTransactionId: "TXN-1"}
}

func TestCreateOrderPending(t *testing.T) {
	svc, _, _ := newTestService(t)
	o, err := svc.CreateOrder(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if o.OrderId != "ord-1" || o.SessionId != "sess-1" || o.Status != pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Errorf("order %v", o)
	}
	if o.PlacedAt != nil || o.ProcessingAt != nil || o.ShippedAt != nil || o.DeliveredAt != nil || o.FailedAt != nil {
		t.Errorf("pending order must have no stage timestamps: %v", o)
	}
	if !o.CreatedAt.AsTime().Equal(t0) || !o.EvaluatedAt.AsTime().Equal(t0) {
		t.Errorf("created %v evaluated %v", o.CreatedAt, o.EvaluatedAt)
	}
	if len(o.Items) != 2 || o.Items[0].Name != "Tee" || o.Items[0].UnitPrice.Units != 19 || o.Total.Units != 50 || o.ShippingCost.Units != 4 {
		t.Errorf("items/money not echoed: %v", o)
	}
}

func TestCreateOrderIdempotent(t *testing.T) {
	svc, st, clk := newTestService(t)
	first, err := svc.CreateOrder(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour)
	second, err := svc.CreateOrder(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !second.CreatedAt.AsTime().Equal(first.CreatedAt.AsTime()) || len(st.orders) != 1 {
		t.Errorf("retry created a second order or moved created_at: %v / %d orders", second.CreatedAt, len(st.orders))
	}
	// same id, other content
	other := validRequest()
	other.Email = "grace@example.com"
	_, err = svc.CreateOrder(context.Background(), other)
	wantCode(t, err, codes.AlreadyExists)
	// retry after the order was placed still returns it (status computed, not PENDING)
	if _, err := svc.ConfirmOrder(context.Background(), confirmReq("ord-1")); err != nil {
		t.Fatal(err)
	}
	again, err := svc.CreateOrder(context.Background(), validRequest())
	if err != nil || again.Status != pb.OrderStatus_ORDER_STATUS_PLACED {
		t.Errorf("retry after confirm: %v %v", again, err)
	}
}

func TestCreateOrderCreatedFlag(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	first, err := svc.CreateOrder(ctx, validRequest())
	if err != nil || first.Created == nil || !*first.Created {
		t.Fatalf("first call must report created=true: %v %v", first, err)
	}
	replay, err := svc.CreateOrder(ctx, validRequest())
	if err != nil || replay.Created == nil || *replay.Created {
		t.Errorf("replay must report created=false (present, not unset): %v %v", replay, err)
	}
	other := validRequest()
	other.Email = "grace@example.com"
	if _, err := svc.CreateOrder(ctx, other); status.Code(err) != codes.AlreadyExists {
		t.Errorf("other content: %v", err)
	}
	// no other RPC sets the flag (it stays unset, not false)
	conf, err := svc.ConfirmOrder(ctx, confirmReq("ord-1"))
	if err != nil || conf.Created != nil {
		t.Errorf("ConfirmOrder: created %v err %v", conf.GetCreated(), err)
	}
	got, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"})
	if err != nil || got.Created != nil {
		t.Errorf("GetOrder: created %v err %v", got.GetCreated(), err)
	}
	list, err := svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-1"})
	if err != nil || len(list.Orders) != 1 || list.Orders[0].Created != nil {
		t.Errorf("ListOrders: %v %v", list, err)
	}
	r2 := validRequest()
	r2.OrderId = "ord-2"
	if _, err := svc.CreateOrder(ctx, r2); err != nil {
		t.Fatal(err)
	}
	failed, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-2", Reason: "declined"})
	if err != nil || failed.Created != nil {
		t.Errorf("FailOrder: created %v err %v", failed.GetCreated(), err)
	}
}

func TestCreateOrderCreatedFlagConcurrent(t *testing.T) {
	svc, _, _ := newTestService(t)
	const n = 20
	var wg sync.WaitGroup
	var trues atomic.Int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if o, err := svc.CreateOrder(context.Background(), validRequest()); err == nil && o.GetCreated() {
				trues.Add(1)
			}
		}()
	}
	wg.Wait()
	if trues.Load() != 1 {
		t.Errorf("created=true on %d of %d parallel identical calls, want exactly 1", trues.Load(), n)
	}
}

func TestStatusAdvancesWithTheClock(t *testing.T) {
	svc, _, clk := newTestService(t)
	ctx := context.Background()
	if _, err := svc.CreateOrder(ctx, validRequest()); err != nil {
		t.Fatal(err)
	}
	// PENDING never advances
	clk.Advance(24 * time.Hour)
	o, _ := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"})
	if o.Status != pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("pending after a day: %v", o.Status)
	}
	placedAt := clk.Now()
	o, err := svc.ConfirmOrder(ctx, confirmReq("ord-1"))
	if err != nil {
		t.Fatal(err)
	}
	if o.ShippingTrackingId != "TRK-1" || o.PaymentTransactionId != "TXN-1" {
		t.Errorf("references: %v", o)
	}
	steps := []struct {
		after time.Duration // since confirmation
		want  pb.OrderStatus
	}{
		{0, pb.OrderStatus_ORDER_STATUS_PLACED},
		{59 * time.Second, pb.OrderStatus_ORDER_STATUS_PLACED},
		{time.Minute, pb.OrderStatus_ORDER_STATUS_PROCESSING},
		{3*time.Minute - time.Second, pb.OrderStatus_ORDER_STATUS_PROCESSING},
		{3 * time.Minute, pb.OrderStatus_ORDER_STATUS_SHIPPED},
		{6*time.Minute - time.Second, pb.OrderStatus_ORDER_STATUS_SHIPPED},
		{6 * time.Minute, pb.OrderStatus_ORDER_STATUS_DELIVERED},
		{72 * time.Hour, pb.OrderStatus_ORDER_STATUS_DELIVERED},
	}
	for _, s := range steps {
		clk.t = placedAt.Add(s.after)
		o, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"})
		if err != nil {
			t.Fatal(err)
		}
		if o.Status != s.want {
			t.Errorf("%v after confirmation: %v, want %v", s.after, o.Status, s.want)
		}
		if !o.PlacedAt.AsTime().Equal(placedAt) || !o.ProcessingAt.AsTime().Equal(placedAt.Add(time.Minute)) ||
			!o.ShippedAt.AsTime().Equal(placedAt.Add(3*time.Minute)) || !o.DeliveredAt.AsTime().Equal(placedAt.Add(6*time.Minute)) {
			t.Errorf("%v: stage timestamps %v %v %v %v", s.after, o.PlacedAt, o.ProcessingAt, o.ShippedAt, o.DeliveredAt)
		}
		if !o.EvaluatedAt.AsTime().Equal(clk.Now()) {
			t.Errorf("evaluated_at %v, want %v", o.EvaluatedAt, clk.Now())
		}
	}
	// the list shows the same computed status
	clk.t = placedAt.Add(4 * time.Minute)
	l, err := svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-1"})
	if err != nil || len(l.Orders) != 1 || l.Orders[0].Status != pb.OrderStatus_ORDER_STATUS_SHIPPED {
		t.Errorf("list: %v %v", l, err)
	}
}

func TestConfirmOrder(t *testing.T) {
	svc, _, clk := newTestService(t)
	ctx := context.Background()
	_, err := svc.ConfirmOrder(ctx, confirmReq("nope"))
	wantCode(t, err, codes.NotFound)
	if _, err := svc.CreateOrder(ctx, validRequest()); err != nil {
		t.Fatal(err)
	}
	first, err := svc.ConfirmOrder(ctx, confirmReq("ord-1"))
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second)
	second, err := svc.ConfirmOrder(ctx, confirmReq("ord-1")) // idempotent
	if err != nil || !second.PlacedAt.AsTime().Equal(first.PlacedAt.AsTime()) {
		t.Errorf("retry: %v %v", second, err)
	}
	bad := confirmReq("ord-1")
	bad.ShippingTrackingId = "OTHER"
	_, err = svc.ConfirmOrder(ctx, bad)
	wantCode(t, err, codes.FailedPrecondition)
	_, err = svc.ConfirmOrder(ctx, &pb.ConfirmOrderRequest{OrderId: "ord-1"})
	wantCode(t, err, codes.InvalidArgument)
}

func TestFailOrder(t *testing.T) {
	svc, _, clk := newTestService(t)
	ctx := context.Background()
	_, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "nope", Reason: "x"})
	wantCode(t, err, codes.NotFound)
	if _, err := svc.CreateOrder(ctx, validRequest()); err != nil {
		t.Fatal(err)
	}
	o, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-1", Reason: "card declined"})
	if err != nil || o.Status != pb.OrderStatus_ORDER_STATUS_FAILED || o.FailureReason != "card declined" || o.FailedAt == nil || o.PlacedAt != nil {
		t.Fatalf("fail: %v %v", o, err)
	}
	clk.Advance(48 * time.Hour) // FAILED never advances
	o, _ = svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"})
	if o.Status != pb.OrderStatus_ORDER_STATUS_FAILED {
		t.Errorf("failed after 2 days: %v", o.Status)
	}
	again, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-1", Reason: "second reason"})
	if err != nil || again.FailureReason != "card declined" {
		t.Errorf("idempotent fail: %v %v", again, err)
	}
	_, err = svc.ConfirmOrder(ctx, confirmReq("ord-1"))
	wantCode(t, err, codes.FailedPrecondition)

	// a placed order cannot fail
	r := validRequest()
	r.OrderId = "ord-2"
	if _, err := svc.CreateOrder(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ConfirmOrder(ctx, confirmReq("ord-2")); err != nil {
		t.Fatal(err)
	}
	_, err = svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-2", Reason: "late"})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "ord-2"})
	wantCode(t, err, codes.InvalidArgument)
}

func TestGetOrderGuards(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	r := validRequest()
	r.UserId = "user-9"
	if _, err := svc.CreateOrder(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		req  *pb.GetOrderRequest
		code codes.Code
	}{
		{"no guard", &pb.GetOrderRequest{OrderId: "ord-1"}, codes.OK},
		{"own session", &pb.GetOrderRequest{OrderId: "ord-1", SessionId: "sess-1"}, codes.OK},
		{"own user", &pb.GetOrderRequest{OrderId: "ord-1", UserId: "user-9"}, codes.OK},
		{"both", &pb.GetOrderRequest{OrderId: "ord-1", SessionId: "sess-1", UserId: "user-9"}, codes.OK},
		{"foreign session", &pb.GetOrderRequest{OrderId: "ord-1", SessionId: "sess-2"}, codes.NotFound},
		{"foreign user", &pb.GetOrderRequest{OrderId: "ord-1", UserId: "user-1"}, codes.NotFound},
		{"own session foreign user", &pb.GetOrderRequest{OrderId: "ord-1", SessionId: "sess-1", UserId: "user-1"}, codes.NotFound},
		{"unknown id", &pb.GetOrderRequest{OrderId: "zzz", SessionId: "sess-1"}, codes.NotFound},
		{"empty id", &pb.GetOrderRequest{SessionId: "sess-1"}, codes.InvalidArgument},
	} {
		t.Run(c.name, func(t *testing.T) {
			o, err := svc.GetOrder(ctx, c.req)
			wantCode(t, err, c.code)
			if c.code != codes.OK && o != nil {
				t.Error("order leaked next to an error")
			}
		})
	}
	// an unknown id and a foreign order must be indistinguishable
	_, e1 := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1", SessionId: "sess-2"})
	_, e2 := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "zzz", SessionId: "sess-2"})
	if e1.Error() != e2.Error() {
		t.Errorf("foreign %q vs unknown %q", e1, e2)
	}
}

func TestListOrders(t *testing.T) {
	svc, _, clk := newTestService(t)
	ctx := context.Background()
	mk := func(id, session, user string) {
		r := validRequest()
		r.OrderId, r.SessionId, r.UserId = id, session, user
		if _, err := svc.CreateOrder(ctx, r); err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Second)
	}
	for i := 1; i <= 5; i++ {
		mk(fmt.Sprintf("a%d", i), "sess-a", "")
	}
	mk("b1", "sess-b", "u1")
	mk("b2", "sess-c", "u1") // same user, other session
	// a1 placed, a2 failed, the rest pending
	if _, err := svc.ConfirmOrder(ctx, confirmReq("a1")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "a2", Reason: "declined"}); err != nil {
		t.Fatal(err)
	}

	ids := func(r *pb.ListOrdersResponse) []string {
		var out []string
		for _, o := range r.Orders {
			out = append(out, o.OrderId)
		}
		return out
	}
	eq := func(got, want []string) bool { return fmt.Sprint(got) == fmt.Sprint(want) }

	r, err := svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a"})
	if err != nil || r.Total != 5 || !eq(ids(r), []string{"a5", "a4", "a3", "a2", "a1"}) {
		t.Errorf("session list newest first: %v %v", ids(r), err)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", Page: 2, PageSize: 2})
	if r.Total != 5 || !eq(ids(r), []string{"a3", "a2"}) {
		t.Errorf("page 2: %v total %d", ids(r), r.Total)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", Page: 9})
	if r.Total != 5 || len(r.Orders) != 0 || r.Orders == nil {
		t.Errorf("past the end: %v", r)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "sess-a", PlacedOnly: true})
	if r.Total != 1 || !eq(ids(r), []string{"a1"}) {
		t.Errorf("placed_only: %v total %d", ids(r), r.Total)
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{UserId: "u1"})
	if r.Total != 2 || !eq(ids(r), []string{"b2", "b1"}) {
		t.Errorf("by user: %v", ids(r))
	}
	r, _ = svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "nobody"})
	if r.Total != 0 || len(r.Orders) != 0 {
		t.Errorf("unknown session: %v", r)
	}
	for _, bad := range []*pb.ListOrdersRequest{{}, {SessionId: "a", UserId: "b"}, {SessionId: "a", Page: -1}, {SessionId: "a", PageSize: -1}} {
		_, err := svc.ListOrders(ctx, bad)
		wantCode(t, err, codes.InvalidArgument)
	}
}

func TestNotReadyAndStorageErrors(t *testing.T) {
	svc, st, _ := newTestService(t)
	ctx := context.Background()
	svc.SetReady(false)
	for name, f := range map[string]func() error{
		"create":  func() error { _, e := svc.CreateOrder(ctx, validRequest()); return e },
		"confirm": func() error { _, e := svc.ConfirmOrder(ctx, confirmReq("o")); return e },
		"fail":    func() error { _, e := svc.FailOrder(ctx, &pb.FailOrderRequest{OrderId: "o", Reason: "r"}); return e },
		"get":     func() error { _, e := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "o"}); return e },
		"list":    func() error { _, e := svc.ListOrders(ctx, &pb.ListOrdersRequest{SessionId: "s"}); return e },
		"claim": func() error {
			_, e := svc.ClaimOrders(ctx, &pb.ClaimOrdersRequest{SessionId: "s", UserId: "u"})
			return e
		},
	} {
		if status.Code(f()) != codes.Unavailable {
			t.Errorf("%s while not ready: %v", name, f())
		}
	}
	// validation answers before readiness, so a bad request is never reported as UNAVAILABLE
	_, err := svc.GetOrder(ctx, &pb.GetOrderRequest{})
	wantCode(t, err, codes.InvalidArgument)

	svc.SetReady(true)
	for _, c := range []struct {
		err  error
		code codes.Code
	}{
		{ErrUnavailable, codes.Unavailable},
		{fmt.Errorf("wrapped: %w", ErrUnavailable), codes.Unavailable},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("ERROR: relation \"orders\" does not exist (SQLSTATE 42P01)"), codes.Internal},
	} {
		st.err = c.err
		_, err := svc.CreateOrder(ctx, validRequest())
		wantCode(t, err, c.code)
		if c.code == codes.Internal && status.Convert(err).Message() != "internal error" {
			t.Errorf("SQL detail leaked: %q", status.Convert(err).Message())
		}
	}
}

func TestNilRequestsDoNotPanic(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	if _, err := svc.CreateOrder(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateOrder(nil): %v", err)
	}
	if _, err := svc.ConfirmOrder(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ConfirmOrder(nil): %v", err)
	}
	if _, err := svc.FailOrder(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("FailOrder(nil): %v", err)
	}
	if _, err := svc.GetOrder(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetOrder(nil): %v", err)
	}
	if _, err := svc.ListOrders(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListOrders(nil): %v", err)
	}
	if _, err := svc.ClaimOrders(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ClaimOrders(nil): %v", err)
	}
}

func TestConcurrentCreateAndConfirm(t *testing.T) {
	svc, st, _ := newTestService(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = svc.CreateOrder(ctx, validRequest())
			_, _ = svc.ConfirmOrder(ctx, confirmReq("ord-1"))
			_, _ = svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1"})
		}()
	}
	wg.Wait()
	if len(st.orders) != 1 || st.orders["ord-1"].State != StatePlaced {
		t.Errorf("orders %d, state %s", len(st.orders), st.orders["ord-1"].State)
	}
}

func TestClaimOrders(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	for _, id := range []string{"ord-1", "ord-2"} {
		r := validRequest()
		r.OrderId = id
		if _, err := svc.CreateOrder(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	other := validRequest()
	other.OrderId, other.SessionId = "ord-3", "sess-2"
	if _, err := svc.CreateOrder(ctx, other); err != nil {
		t.Fatal(err)
	}

	claim := func(session, user string) int32 {
		t.Helper()
		resp, err := svc.ClaimOrders(ctx, &pb.ClaimOrdersRequest{SessionId: session, UserId: user})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Claimed
	}
	if n := claim("sess-1", "user-1"); n != 2 {
		t.Fatalf("first claim = %d, want 2", n)
	}
	if n := claim("sess-1", "user-1"); n != 0 {
		t.Errorf("repeated claim = %d, want 0", n)
	}
	if n := claim("sess-1", "user-2"); n != 0 {
		t.Errorf("claim by a second user = %d, want 0", n)
	}
	if n := claim("nobody", "user-2"); n != 0 {
		t.Errorf("claim of an unknown session = %d, want 0", n)
	}
	got, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1", UserId: "user-1"})
	if err != nil || got.UserId != "user-1" || got.SessionId != "sess-1" {
		t.Fatalf("claimed order: %v, %v", got, err)
	}
	_, err = svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-1", UserId: "user-2"})
	wantCode(t, err, codes.NotFound)
	// the other session was not touched
	if o, err := svc.GetOrder(ctx, &pb.GetOrderRequest{OrderId: "ord-3"}); err != nil || o.UserId != "" {
		t.Errorf("order of another session: %v, %v", o, err)
	}
}

func TestClaimOrdersInvalid(t *testing.T) {
	svc, _, _ := newTestService(t)
	for name, req := range map[string]*pb.ClaimOrdersRequest{
		"empty":          {},
		"no user":        {SessionId: "s"},
		"no session":     {UserId: "u"},
		"blank user":     {SessionId: "s", UserId: "   "},
		"long session":   {SessionId: strings.Repeat("s", 129), UserId: "u"},
		"long user":      {SessionId: "s", UserId: strings.Repeat("u", 129)},
		"nul in session": {SessionId: "s\x00", UserId: "u"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := svc.ClaimOrders(context.Background(), req)
			wantCode(t, err, codes.InvalidArgument)
		})
	}
	// the limits themselves are accepted
	_, err := svc.ClaimOrders(context.Background(), &pb.ClaimOrdersRequest{SessionId: strings.Repeat("s", 128), UserId: strings.Repeat("u", 128)})
	if err != nil {
		t.Errorf("128 characters: %v", err)
	}
}

func TestClaimOrdersStorageError(t *testing.T) {
	svc, st, _ := newTestService(t)
	st.err = errors.New("ERROR: boom (SQLSTATE 42P01)")
	_, err := svc.ClaimOrders(context.Background(), &pb.ClaimOrdersRequest{SessionId: "s", UserId: "u"})
	wantCode(t, err, codes.Internal)
	if status.Convert(err).Message() != "internal error" {
		t.Errorf("SQL detail leaked: %q", status.Convert(err).Message())
	}
	st.err = ErrUnavailable
	_, err = svc.ClaimOrders(context.Background(), &pb.ClaimOrdersRequest{SessionId: "s", UserId: "u"})
	wantCode(t, err, codes.Unavailable)
}
