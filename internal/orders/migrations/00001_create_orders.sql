-- +goose Up
-- One row per order. The stored state is PENDING, PLACED or FAILED; PROCESSING/SHIPPED/DELIVERED
-- are computed from confirmed_at on every read and never stored.
CREATE TABLE orders (
    id             text        PRIMARY KEY CHECK (char_length(id) BETWEEN 1 AND 64),
    session_id     text        NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 128),
    user_id        text        NOT NULL DEFAULT '' CHECK (char_length(user_id) <= 128),
    email          text        NOT NULL CHECK (char_length(email) BETWEEN 3 AND 254),
    ship_street    text        NOT NULL CHECK (char_length(ship_street) BETWEEN 1 AND 200),
    ship_city      text        NOT NULL CHECK (char_length(ship_city) BETWEEN 1 AND 100),
    ship_state     text        NOT NULL CHECK (char_length(ship_state) <= 100),
    ship_country   text        NOT NULL CHECK (char_length(ship_country) BETWEEN 1 AND 100),
    ship_zip       integer     NOT NULL CHECK (ship_zip >= 0),
    currency       text        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    shipping_units bigint      NOT NULL CHECK (shipping_units >= 0),
    shipping_nanos integer     NOT NULL CHECK (shipping_nanos BETWEEN 0 AND 999999999),
    total_units    bigint      NOT NULL CHECK (total_units >= 0),
    total_nanos    integer     NOT NULL CHECK (total_nanos BETWEEN 0 AND 999999999),
    state          text        NOT NULL CHECK (state IN ('PENDING', 'PLACED', 'FAILED')),
    failure_reason text        NOT NULL DEFAULT '',
    tracking_id    text        NOT NULL DEFAULT '',
    transaction_id text        NOT NULL DEFAULT '',
    fingerprint    text        NOT NULL,
    created_at     timestamptz NOT NULL,
    confirmed_at   timestamptz,
    failed_at      timestamptz,
    CHECK (state <> 'PLACED' OR (confirmed_at IS NOT NULL AND tracking_id <> '' AND transaction_id <> '')),
    CHECK (state <> 'FAILED' OR failed_at IS NOT NULL)
);

CREATE TABLE order_items (
    order_id    text    NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    position    integer NOT NULL CHECK (position >= 0),
    product_id  text    NOT NULL CHECK (char_length(product_id) BETWEEN 1 AND 128),
    name        text    NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    quantity    integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_units  bigint  NOT NULL CHECK (unit_units >= 0),
    unit_nanos  integer NOT NULL CHECK (unit_nanos BETWEEN 0 AND 999999999),
    picture     text    NOT NULL DEFAULT '' CHECK (char_length(picture) <= 500),
    PRIMARY KEY (order_id, position)
);

-- ListOrders: one owner, newest first (id breaks ties so paging is stable)
CREATE INDEX orders_session_created_idx ON orders (session_id, created_at DESC, id DESC);
CREATE INDEX orders_user_created_idx ON orders (user_id, created_at DESC, id DESC) WHERE user_id <> '';

-- +goose Down
DROP TABLE order_items;
DROP TABLE orders;
