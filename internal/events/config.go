package events

import "strings"

// Config is the consumer's environment: KAFKA_BROKERS (comma separated, empty = Kafka off) and
// KAFKA_CONSUMER_GROUP (default "order-service").
type Config struct {
	Brokers []string
	Group   string
}

// DefaultGroup is the consumer group when KAFKA_CONSUMER_GROUP is not set.
const DefaultGroup = "order-service"

// ConfigFromEnv reads the configuration; getenv is os.Getenv in production.
func ConfigFromEnv(getenv func(string) string) Config {
	c := Config{Group: strings.TrimSpace(getenv("KAFKA_CONSUMER_GROUP"))}
	if c.Group == "" {
		c.Group = DefaultGroup
	}
	for _, b := range strings.Split(getenv("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Brokers = append(c.Brokers, b)
		}
	}
	return c
}

// Enabled reports whether a consumer should run at all.
func (c Config) Enabled() bool { return len(c.Brokers) > 0 }
