package main

import (
	"fmt"
	"strings"
	"time"

	"order-service/internal/orders"
)

// stagesFromEnv reads the offsets after confirmation at which the shown order status advances:
// ORDER_PROCESSING_AFTER (default 1m), ORDER_SHIPPED_AFTER (3m), ORDER_DELIVERED_AFTER (6m), Go durations
// such as "90s" or "2m". They must satisfy 0 < processing < shipped < delivered.
func stagesFromEnv(getenv func(string) string) (orders.Stages, error) {
	s := orders.DefaultStages
	for _, f := range []struct {
		name string
		dst  *time.Duration
	}{
		{"ORDER_PROCESSING_AFTER", &s.Processing},
		{"ORDER_SHIPPED_AFTER", &s.Shipped},
		{"ORDER_DELIVERED_AFTER", &s.Delivered},
	} {
		v := strings.TrimSpace(getenv(f.name))
		if v == "" {
			continue
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return orders.Stages{}, fmt.Errorf("%s: %w", f.name, err)
		}
		*f.dst = d
	}
	if err := s.Validate(); err != nil {
		return orders.Stages{}, err
	}
	return s, nil
}
