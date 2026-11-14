package events

import (
	"fmt"
	"regexp"
	"strings"
)

// Config is the consumer's environment: KAFKA_BROKERS (comma separated, empty = Kafka off),
// KAFKA_CONSUMER_GROUP (default "order-service") and KAFKA_TOPIC_PREFIX (default empty).
type Config struct {
	Brokers []string
	Group   string
	// TopicPrefix is put in front of the topic name ("dev." gives "dev.orders.placed") so several environments can
	// share one Kafka cluster (FUT-03). The consumer group is NOT prefixed: group offsets are kept per topic.
	TopicPrefix string
	// Topic is TopicPrefix + Topic ("orders.placed") unless set; only tests set it directly.
	Topic string
}

// topicChars are the characters Kafka allows in a topic name; maxTopicLen is its length limit.
var topicChars = regexp.MustCompile(`^[a-zA-Z0-9._-]*$`)

const maxTopicLen = 249

// Validate rejects a prefix that cannot be part of a Kafka topic name (checked at start, so a typo is a one-line
// error and not a consumer that silently never starts).
func (c Config) Validate() error {
	if !topicChars.MatchString(c.TopicPrefix) {
		return fmt.Errorf("KAFKA_TOPIC_PREFIX %q: only letters, digits, '.', '_' and '-' are allowed", c.TopicPrefix)
	}
	if len(c.TopicPrefix)+len(Topic) > maxTopicLen {
		return fmt.Errorf("KAFKA_TOPIC_PREFIX is too long: the topic name may have at most %d characters", maxTopicLen)
	}
	return nil
}

// DefaultGroup is the consumer group when KAFKA_CONSUMER_GROUP is not set.
const DefaultGroup = "order-service"

// ConfigFromEnv reads the configuration; getenv is os.Getenv in production.
func ConfigFromEnv(getenv func(string) string) Config {
	c := Config{Group: strings.TrimSpace(getenv("KAFKA_CONSUMER_GROUP")), TopicPrefix: strings.TrimSpace(getenv("KAFKA_TOPIC_PREFIX"))}
	c.Topic = c.TopicPrefix + Topic
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
