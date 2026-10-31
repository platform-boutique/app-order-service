package orders

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	pb "order-service/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements the gRPC OrderService.
type Service struct {
	pb.UnimplementedOrderServiceServer
	store  Store
	log    *logrus.Logger
	stages Stages
	now    func() time.Time
	ready  atomic.Bool
}

// NewService creates the service. stages decide when the shown status advances; now is the clock
// (time.Now in production, a fake in tests). It answers UNAVAILABLE until SetReady(true).
func NewService(store Store, log *logrus.Logger, stages Stages, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, log: log, stages: stages, now: now}
}

// SetReady switches the handlers on or off.
func (s *Service) SetReady(ok bool) { s.ready.Store(ok) }

func (s *Service) checkReady() error {
	if !s.ready.Load() {
		return status.Error(codes.Unavailable, "database not ready")
	}
	return nil
}

// clock returns the current time in UTC, cut to Postgres' microsecond precision so that a stored value
// and the one returned to the caller are identical.
func (s *Service) clock() time.Time { return s.now().UTC().Truncate(time.Microsecond) }

// toStatus maps internal errors to gRPC status codes without leaking SQL details to the caller.
func (s *Service) toStatus(method string, err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "order not found")
	case errors.Is(err, ErrConflict):
		return status.Error(codes.AlreadyExists, "an order with this order_id already exists with different content")
	case errors.Is(err, ErrPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, ErrUnavailable):
		s.log.Warnf("[%s] database unavailable: %v", method, err)
		return status.Error(codes.Unavailable, "database unavailable")
	default:
		s.log.Errorf("[%s] storage error: %v", method, err)
		return status.Error(codes.Internal, "internal error")
	}
}

var statusToPB = map[Status]pb.OrderStatus{
	StatusPending:    pb.OrderStatus_ORDER_STATUS_PENDING,
	StatusPlaced:     pb.OrderStatus_ORDER_STATUS_PLACED,
	StatusProcessing: pb.OrderStatus_ORDER_STATUS_PROCESSING,
	StatusShipped:    pb.OrderStatus_ORDER_STATUS_SHIPPED,
	StatusDelivered:  pb.OrderStatus_ORDER_STATUS_DELIVERED,
	StatusFailed:     pb.OrderStatus_ORDER_STATUS_FAILED,
}

func moneyPB(m Money) *pb.Money {
	return &pb.Money{CurrencyCode: m.Currency, Units: m.Units, Nanos: m.Nanos}
}

func tsPB(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

// toPB renders a stored order with the status computed for the instant now.
func (s *Service) toPB(o Order, now time.Time) *pb.Order {
	p := s.stages.Schedule(o, now)
	out := &pb.Order{
		OrderId:   o.ID,
		SessionId: o.SessionID,
		UserId:    o.UserID,
		Email:     o.Email,
		ShippingAddress: &pb.Address{
			StreetAddress: o.Address.Street, City: o.Address.City, State: o.Address.State,
			Country: o.Address.Country, ZipCode: o.Address.Zip,
		},
		Items:                make([]*pb.OrderItem, 0, len(o.Items)),
		ShippingCost:         moneyPB(o.ShippingCost),
		Total:                moneyPB(o.Total),
		Status:               statusToPB[p.Status],
		FailureReason:        o.FailureReason,
		ShippingTrackingId:   o.TrackingID,
		PaymentTransactionId: o.TransactionID,
		CreatedAt:            timestamppb.New(o.CreatedAt),
		PlacedAt:             tsPB(p.PlacedAt),
		ProcessingAt:         tsPB(p.ProcessingAt),
		ShippedAt:            tsPB(p.ShippedAt),
		DeliveredAt:          tsPB(p.DeliveredAt),
		FailedAt:             tsPB(o.FailedAt),
		EvaluatedAt:          timestamppb.New(now),
	}
	for _, it := range o.Items {
		out.Items = append(out.Items, &pb.OrderItem{
			ProductId: it.ProductID, Name: it.Name, Quantity: it.Quantity, UnitPrice: moneyPB(it.UnitPrice), Picture: it.Picture,
		})
	}
	return out
}

func clampInt32(v int64) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v) //#nosec G115 -- bounded above
}

// CreateOrder stores a PENDING order; repeating it with the same content returns the stored order.
func (s *Service) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.Order, error) {
	n, err := ValidateNewOrder(req)
	if err != nil {
		return nil, s.toStatus("CreateOrder", err)
	}
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	now := s.clock()
	o, created, err := s.store.Create(ctx, n, now)
	if err != nil {
		return nil, s.toStatus("CreateOrder", err)
	}
	s.log.WithFields(logrus.Fields{"order_id": o.ID, "items": len(o.Items), "created": created}).Info("[CreateOrder] order stored")
	return s.toPB(o, now), nil
}

// ConfirmOrder turns a PENDING order into PLACED.
func (s *Service) ConfirmOrder(ctx context.Context, req *pb.ConfirmOrderRequest) (*pb.Order, error) {
	id, tracking, txn, err := ValidateConfirm(req)
	if err != nil {
		return nil, s.toStatus("ConfirmOrder", err)
	}
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	now := s.clock()
	o, err := s.store.Confirm(ctx, id, tracking, txn, now)
	if err != nil {
		return nil, s.toStatus("ConfirmOrder", err)
	}
	s.log.WithFields(logrus.Fields{"order_id": o.ID}).Info("[ConfirmOrder] order placed")
	return s.toPB(o, now), nil
}

// FailOrder turns a PENDING order into FAILED.
func (s *Service) FailOrder(ctx context.Context, req *pb.FailOrderRequest) (*pb.Order, error) {
	id, reason, err := ValidateFail(req)
	if err != nil {
		return nil, s.toStatus("FailOrder", err)
	}
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	now := s.clock()
	o, err := s.store.Fail(ctx, id, reason, now)
	if err != nil {
		return nil, s.toStatus("FailOrder", err)
	}
	s.log.WithFields(logrus.Fields{"order_id": o.ID}).Info("[FailOrder] order failed")
	return s.toPB(o, now), nil
}

// GetOrder returns one order; a session_id/user_id that does not match answers NOT_FOUND.
func (s *Service) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.Order, error) {
	id, sessionGuard, userGuard, err := ValidateGet(req)
	if err != nil {
		return nil, s.toStatus("GetOrder", err)
	}
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	o, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, s.toStatus("GetOrder", err)
	}
	if (sessionGuard != "" && sessionGuard != o.SessionID) || (userGuard != "" && userGuard != o.UserID) {
		return nil, s.toStatus("GetOrder", ErrNotFound)
	}
	return s.toPB(o, s.clock()), nil
}

// ListOrders returns one page of the orders of one session or user, newest first.
func (s *Service) ListOrders(ctx context.Context, req *pb.ListOrdersRequest) (*pb.ListOrdersResponse, error) {
	f, limit, offset, err := ValidateList(req)
	if err != nil {
		return nil, s.toStatus("ListOrders", err)
	}
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	page, total, err := s.store.List(ctx, f, limit, offset)
	if err != nil {
		return nil, s.toStatus("ListOrders", err)
	}
	now := s.clock()
	resp := &pb.ListOrdersResponse{Orders: make([]*pb.Order, 0, len(page)), Total: clampInt32(total)}
	for _, o := range page {
		resp.Orders = append(resp.Orders, s.toPB(o, now))
	}
	return resp, nil
}

// ClaimOrders gives the unowned orders of an anonymous session to a user. Idempotent; never touches orders that
// already have a user_id.
func (s *Service) ClaimOrders(ctx context.Context, req *pb.ClaimOrdersRequest) (*pb.ClaimOrdersResponse, error) {
	sessionID, userID, err := ValidateClaim(req)
	if err != nil {
		return nil, s.toStatus("ClaimOrders", err)
	}
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	n, err := s.store.Claim(ctx, sessionID, userID)
	if err != nil {
		return nil, s.toStatus("ClaimOrders", err)
	}
	// only the count is logged: the session id is a browser cookie value and stays out of the logs
	s.log.WithFields(logrus.Fields{"claimed": n}).Info("[ClaimOrders] orders claimed")
	return &pb.ClaimOrdersResponse{Claimed: clampInt32(n)}, nil
}
