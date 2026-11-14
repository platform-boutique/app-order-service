package events

import (
	"strings"
	"testing"
)

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestConfigDefaultsAreUnchanged(t *testing.T) {
	c := ConfigFromEnv(envOf(map[string]string{"KAFKA_BROKERS": "k1:9092, k2:9092"}))
	if c.Topic != "orders.placed" || c.Group != "order-service" || c.TopicPrefix != "" {
		t.Fatalf("defaults changed: %+v", c)
	}
	if len(c.Brokers) != 2 || !c.Enabled() {
		t.Fatalf("brokers: %+v", c)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigTopicPrefix(t *testing.T) {
	c := ConfigFromEnv(envOf(map[string]string{"KAFKA_BROKERS": "k:9092", "KAFKA_TOPIC_PREFIX": " dev. "}))
	if c.Topic != "dev.orders.placed" || c.TopicPrefix != "dev." {
		t.Fatalf("topic = %q prefix = %q", c.Topic, c.TopicPrefix)
	}
	if c.Group != "order-service" {
		t.Errorf("the group must not be prefixed, got %q", c.Group)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsAnImpossibleTopicPrefix(t *testing.T) {
	for _, p := range []string{"dev/", "a b.", "dev:", "ü", strings.Repeat("x", 240)} {
		c := ConfigFromEnv(envOf(map[string]string{"KAFKA_BROKERS": "k:9092", "KAFKA_TOPIC_PREFIX": p}))
		if err := c.Validate(); err == nil {
			t.Errorf("prefix %q was accepted", p)
		}
	}
}
