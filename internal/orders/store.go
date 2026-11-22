package orders

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is what the gRPC handlers need from the storage layer.
type Store interface {
	// Create stores a new PENDING order. If the id exists with the same content it returns the stored
	// order and created=false; with different content ErrConflict.
	Create(ctx context.Context, n NewOrder, now time.Time) (o Order, created bool, err error)
	// Get returns an order or ErrNotFound.
	Get(ctx context.Context, id string) (Order, error)
	// Confirm applies ApplyConfirm atomically (ErrNotFound, ErrPrecondition).
	Confirm(ctx context.Context, id, tracking, txn string, now time.Time) (Order, error)
	// Fail applies ApplyFail atomically (ErrNotFound, ErrPrecondition).
	Fail(ctx context.Context, id, reason string, now time.Time) (Order, error)
	// List returns one page of the matching orders, newest first, plus the total number of matches.
	List(ctx context.Context, f Filter, limit, offset int64) (page []Order, total int64, err error)
	// Claim sets user_id on every order of the session whose user_id is empty (one conditional UPDATE) and
	// returns the number of rows changed. Orders that already have a user are never touched.
	Claim(ctx context.Context, sessionID, userID string) (int64, error)
	// Ping checks that the database answers.
	Ping(ctx context.Context) error
}

// ErrUnavailable marks storage errors caused by an unreachable database; the gRPC layer maps it to UNAVAILABLE.
var ErrUnavailable = errors.New("database unavailable")

// PGStore is the Postgres implementation of Store.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore wraps an existing pool (the caller owns and closes it).
func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrNotFound) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// the server answered: a real SQL error, except for "server shutting down / too many connections" classes
		if strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "57") || pgErr.Code == "53300" {
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		return err
	}
	// anything else (dial errors, closed connections, timeouts) means the database is not reachable
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}

// querier is what both *pgxpool.Pool and pgx.Tx offer.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const orderColumns = `id, session_id, user_id, email, ship_street, ship_city, ship_state, ship_country, ship_zip,
	currency, shipping_units, shipping_nanos, total_units, total_nanos,
	state, failure_reason, tracking_id, transaction_id, fingerprint, created_at, confirmed_at, failed_at`

func scanOrder(row pgx.Row) (Order, error) {
	var o Order
	var cur string
	var shipU, totU int64
	var shipN, totN int32
	err := row.Scan(&o.ID, &o.SessionID, &o.UserID, &o.Email,
		&o.Address.Street, &o.Address.City, &o.Address.State, &o.Address.Country, &o.Address.Zip,
		&cur, &shipU, &shipN, &totU, &totN,
		&o.State, &o.FailureReason, &o.TrackingID, &o.TransactionID, &o.Fingerprint, &o.CreatedAt, &o.ConfirmedAt, &o.FailedAt)
	if err != nil {
		return Order{}, err
	}
	o.ShippingCost = Money{Currency: cur, Units: shipU, Nanos: shipN}
	o.Total = Money{Currency: cur, Units: totU, Nanos: totN}
	return o, nil
}

// loadItems fills the Items of the given orders with one query.
func loadItems(ctx context.Context, q querier, orders []Order) error {
	if len(orders) == 0 {
		return nil
	}
	ids := make([]string, len(orders))
	index := make(map[string]int, len(orders))
	for i := range orders {
		ids[i] = orders[i].ID
		index[orders[i].ID] = i
	}
	rows, err := q.Query(ctx,
		`SELECT order_id, product_id, name, quantity, unit_units, unit_nanos, picture
		   FROM order_items WHERE order_id = ANY($1) ORDER BY order_id, position`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var oid string
		var it Item
		if err := rows.Scan(&oid, &it.ProductID, &it.Name, &it.Quantity, &it.UnitPrice.Units, &it.UnitPrice.Nanos, &it.Picture); err != nil {
			return err
		}
		o := &orders[index[oid]]
		it.UnitPrice.Currency = o.Total.Currency // one currency per order
		o.Items = append(o.Items, it)
	}
	return rows.Err()
}

// loadOrder reads one order with its items; forUpdate locks the order row until the transaction ends.
func loadOrder(ctx context.Context, q querier, id string, forUpdate bool) (Order, error) {
	sql := `SELECT ` + orderColumns + ` FROM orders WHERE id = $1`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	o, err := scanOrder(q.QueryRow(ctx, sql, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, err
	}
	list := []Order{o}
	if err := loadItems(ctx, q, list); err != nil {
		return Order{}, err
	}
	return list[0], nil
}

// Create implements Store.
func (s *PGStore) Create(ctx context.Context, n NewOrder, now time.Time) (Order, bool, error) {
	o := Order{NewOrder: n, State: StatePending, Fingerprint: n.Fingerprint(), CreatedAt: now}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Order{}, false, classify(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`INSERT INTO orders (id, session_id, user_id, email, ship_street, ship_city, ship_state, ship_country, ship_zip,
		                     currency, shipping_units, shipping_nanos, total_units, total_nanos, state, fingerprint, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'PENDING', $15, $16)
		 ON CONFLICT (id) DO NOTHING`,
		n.ID, n.SessionID, n.UserID, n.Email, n.Address.Street, n.Address.City, n.Address.State, n.Address.Country, n.Address.Zip,
		n.Total.Currency, n.ShippingCost.Units, n.ShippingCost.Nanos, n.Total.Units, n.Total.Nanos, o.Fingerprint, now)
	if err != nil {
		return Order{}, false, classify(err)
	}
	if tag.RowsAffected() == 0 {
		// the id exists (an earlier call, or a concurrent one that has committed by now)
		existing, err := loadOrder(ctx, tx, n.ID, false)
		if err != nil {
			return Order{}, false, classify(err)
		}
		if existing.Fingerprint != o.Fingerprint {
			return Order{}, false, ErrConflict
		}
		return existing, false, nil
	}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"order_items"},
		[]string{"order_id", "position", "product_id", "name", "quantity", "unit_units", "unit_nanos", "picture"},
		pgx.CopyFromSlice(len(n.Items), func(i int) ([]any, error) {
			it := n.Items[i]
			return []any{n.ID, i, it.ProductID, it.Name, it.Quantity, it.UnitPrice.Units, it.UnitPrice.Nanos, it.Picture}, nil
		}))
	if err != nil {
		return Order{}, false, classify(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, false, classify(err)
	}
	ordersTotal.WithLabelValues(eventCreated).Inc()
	return o, true, nil
}

// Get implements Store.
func (s *PGStore) Get(ctx context.Context, id string) (Order, error) {
	o, err := loadOrder(ctx, s.pool, id, false)
	if errors.Is(err, ErrNotFound) {
		return Order{}, err
	}
	return o, classify(err)
}

// transition runs one state change under a row lock, so concurrent Confirm/Fail calls on the same order serialise.
func (s *PGStore) transition(ctx context.Context, id string, apply func(Order) (Order, bool, error)) (Order, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Order{}, classify(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := loadOrder(ctx, tx, id, true)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Order{}, err
		}
		return Order{}, classify(err)
	}
	updated, changed, err := apply(o)
	if err != nil {
		return Order{}, err
	}
	if !changed {
		return updated, nil
	}
	_, err = tx.Exec(ctx,
		`UPDATE orders SET state = $2, failure_reason = $3, tracking_id = $4, transaction_id = $5, confirmed_at = $6, failed_at = $7
		  WHERE id = $1`,
		id, updated.State, updated.FailureReason, updated.TrackingID, updated.TransactionID, updated.ConfirmedAt, updated.FailedAt)
	if err != nil {
		return Order{}, classify(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, classify(err)
	}
	countTransition(updated.State)
	return updated, nil
}

// Confirm implements Store.
func (s *PGStore) Confirm(ctx context.Context, id, tracking, txn string, now time.Time) (Order, error) {
	return s.transition(ctx, id, func(o Order) (Order, bool, error) { return ApplyConfirm(o, tracking, txn, now) })
}

// Fail implements Store.
func (s *PGStore) Fail(ctx context.Context, id, reason string, now time.Time) (Order, error) {
	return s.transition(ctx, id, func(o Order) (Order, bool, error) { return ApplyFail(o, reason, now) })
}

// List implements Store.
func (s *PGStore) List(ctx context.Context, f Filter, limit, offset int64) ([]Order, int64, error) {
	where, args := "session_id = $1", []any{f.SessionID}
	if f.UserID != "" {
		where, args = "user_id = $1", []any{f.UserID}
	}
	if f.PlacedOnly {
		where += " AND state = 'PLACED'"
	}
	var total int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, classify(err)
	}
	if total == 0 || offset >= total {
		return []Order{}, total, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE `+where+` ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3`,
		append(args, limit, offset)...)
	if err != nil {
		return nil, 0, classify(err)
	}
	page, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Order, error) { return scanOrder(row) })
	if err != nil {
		return nil, 0, classify(err)
	}
	if err := loadItems(ctx, s.pool, page); err != nil {
		return nil, 0, classify(err)
	}
	return page, total, nil
}

// Claim implements Store: one statement, so two concurrent claims of the same session cannot both win a row
// (the second UPDATE re-checks the empty user_id after the first one commits).
func (s *PGStore) Claim(ctx context.Context, sessionID, userID string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE orders SET user_id = $2 WHERE session_id = $1 AND user_id = ''`, sessionID, userID)
	if err != nil {
		return 0, classify(err)
	}
	return tag.RowsAffected(), nil
}

// Ping implements Store.
func (s *PGStore) Ping(ctx context.Context) error { return classify(s.pool.Ping(ctx)) }
