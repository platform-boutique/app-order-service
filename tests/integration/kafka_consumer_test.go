//go:build integration

// Integration tests of the orders.placed consumer against a real Kafka and a real Postgres. Run with:
//
//	TEST_DATABASE_URL=postgres://order:order@localhost:55433/postgres?sslmode=disable \
//	TEST_KAFKA_BROKERS=127.0.0.1:19092 \
//	  go test -race -count=1 -tags integration -run Kafka ./tests/integration/
//
// TEST_KAFKA_CONTAINER (the name of the throwaway broker container) additionally enables the broker-restart test.
// Every test creates its own topic (3 partitions, created explicitly and waited for until every partition has a
// leader; no reliance on auto-creation) and uses its own consumer group, so tests do not see each other's records.
// The host clock of the development machine jumps and a loaded broker is slow, so all waits are generous (60 s;
// the broker-restart test 180 s) and only matter when something is really wrong.
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"order-service/internal/events"
	"order-service/internal/orders"
	pb "order-service/proto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const kafkaWait = 60 * time.Second

var warmOnce sync.Once

func kafkaBrokers(t *testing.T) string {
	t.Helper()
	b := os.Getenv("TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	warmOnce.Do(func() { warmUpGroupCoordinator(t, b) })
	return b
}

// warmUpGroupCoordinator waits until the broker can serve consumer groups. A freshly started broker creates and loads
// the 50 partitions of __consumer_offsets on the first group request, and until then answers COORDINATOR_NOT_AVAILABLE
// / NOT_COORDINATOR; on a loaded host that took 10-60 s and made the first test of a run slow. That is the broker's
// warm-up, not the consumer's behaviour, so it is paid here once, outside any test's own waits.
func warmUpGroupCoordinator(t *testing.T, brokers string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*kafkaWait)
	defer cancel()
	group := uniq("warmup")
	var last error
	for ctx.Err() == nil {
		req := kmsg.NewOffsetFetchRequest()
		req.Group = group
		resp, err := req.RequestWith(ctx, cl)
		last = err
		if err == nil {
			last = kerr.ErrorForCode(resp.ErrorCode)
		}
		if last == nil {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("group coordinator not ready: %v", last)
}

var seq atomic.Int64

// uniq returns a name that is unique across tests and runs.
func uniq(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano()%1_000_000_000, seq.Add(1))
}

func eventValue(orderID, tracking, txn string) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"ev-%[1]s","event_type":"order.placed","schema_version":1,"occurred_at":"2026-10-31T18:00:00.123Z",`+
		`"order_id":%[1]q,"user_id":"","email":"ada@example.com","shipping_address":{"street_address":"1 Main St","city":"Zurich","state":"ZH","country":"CH","zip_code":8001},`+
		`"items":[{"product_id":"tee","name":"Tee","quantity":2,"unit_price":{"currency_code":"USD","units":19,"nanos":990000000},"picture":""}],`+
		`"shipping_cost":{"currency_code":"USD","units":4,"nanos":750000000},"total":{"currency_code":"USD","units":50,"nanos":230000000},`+
		`"shipping_tracking_id":%[2]q,"payment_transaction_id":%[3]q,"added_later":true}`, orderID, tracking, txn))
}

// tolerantProducer keeps retrying a topic or leader that is not known yet (a loaded broker needs seconds, kgo's
// default gives up after about one) and has long timeouts.
func tolerantProducer(brokers string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.UnknownTopicRetries(-1),
		kgo.ProduceRequestTimeout(30*time.Second),
		kgo.RecordDeliveryTimeout(kafkaWait),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
}

// createTopic creates the topic explicitly (3 partitions) and returns when the metadata shows a leader for every
// partition. An existing topic is fine.
func createTopic(t *testing.T, brokers, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*kafkaWait)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		last = createTopicOnce(ctx, cl, topic)
		if last == nil {
			last = topicHasLeaders(ctx, cl, topic, 3)
		}
		if last == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("topic %s not ready: %v", topic, last)
}

func createTopicOnce(ctx context.Context, cl *kgo.Client, topic string) error {
	req := kmsg.NewCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 3, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, rs := range resp.Topics {
		if err := kerr.ErrorForCode(rs.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create %s: %w", rs.Topic, err)
		}
	}
	return nil
}

func topicHasLeaders(ctx context.Context, cl *kgo.Client, topic string, partitions int) error {
	req := kmsg.NewMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = &topic
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, mt := range resp.Topics {
		if err := kerr.ErrorForCode(mt.ErrorCode); err != nil {
			return err
		}
		if len(mt.Partitions) != partitions {
			return fmt.Errorf("%d partitions, want %d", len(mt.Partitions), partitions)
		}
		for _, p := range mt.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return err
			}
			if p.Leader < 0 {
				return fmt.Errorf("partition %d has no leader yet", p.Partition)
			}
		}
	}
	return nil
}

// newTopic returns a unique topic that exists and has leaders.
func newTopic(t *testing.T, brokers string) string {
	t.Helper()
	topic := uniq("orders.placed.it")
	createTopic(t, brokers, topic)
	return topic
}

// produce publishes records (key, value) to the (existing) topic and waits for the acks.
func produce(t *testing.T, brokers, topic string, kv ...[2][]byte) {
	t.Helper()
	cl, err := tolerantProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), kafkaWait)
	defer cancel()
	var recs []*kgo.Record
	for _, p := range kv {
		recs = append(recs, &kgo.Record{Topic: topic, Key: p[0], Value: p[1]})
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func produceEvent(t *testing.T, brokers, topic, orderID, tracking, txn string) {
	t.Helper()
	produce(t, brokers, topic, [2][]byte{[]byte(orderID), eventValue(orderID, tracking, txn)})
}

// recordingHandler wraps the real handler and remembers which order ids it was asked to handle.
type recordingHandler struct {
	inner events.Handler
	mu    sync.Mutex
	seen  []string
}

func (h *recordingHandler) Handle(ctx context.Context, m events.Meta, ev events.Event) (events.Result, error) {
	h.mu.Lock()
	h.seen = append(h.seen, ev.OrderID)
	h.mu.Unlock()
	return h.inner.Handle(ctx, m, ev)
}

func (h *recordingHandler) calls() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

type runningConsumer struct {
	g *events.Group
	h *recordingHandler
}

func (c *runningConsumer) stop(t *testing.T) {
	t.Helper()
	if !c.g.Stop(15 * time.Second) {
		t.Error("consumer did not stop within 15 s")
	}
}

func startConsumer(t *testing.T, brokers, topic, group string, svc *orders.Service) *runningConsumer {
	t.Helper()
	l := logrus.New()
	l.Out = io.Discard
	if os.Getenv("TEST_KAFKA_DEBUG") != "" { // kafka client + consumer logs of the test, for diagnosing slow runs
		l.Out = os.Stderr
		l.SetLevel(logrus.DebugLevel)
	}
	src, err := events.NewKafkaSource(events.Config{Brokers: strings.Split(brokers, ","), Group: group, Topic: topic}, l)
	if err != nil {
		t.Fatal(err)
	}
	rh := &recordingHandler{inner: &events.ConfirmHandler{Orders: svc, Log: l}}
	c := &events.Consumer{Source: src, Handler: rh, Log: l, Metrics: events.NewMetrics(prometheus.NewRegistry())}
	rc := &runningConsumer{g: events.Start(context.Background(), c), h: rh}
	t.Cleanup(func() { rc.g.Stop(15 * time.Second) })
	return rc
}

func createPending(t *testing.T, svc *orders.Service, id string) {
	t.Helper()
	if _, err := svc.CreateOrder(context.Background(), request(id, "sess-"+id, "")); err != nil {
		t.Fatal(err)
	}
}

func getOrder(t *testing.T, svc *orders.Service, id string) *pb.Order {
	t.Helper()
	o, err := svc.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: id})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// until polls cond every 100 ms for up to kafkaWait.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	untilWithin(t, kafkaWait, what, cond)
}

func untilWithin(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func isPlaced(t *testing.T, svc *orders.Service, id string) func() bool {
	return func() bool {
		return getOrder(t, svc, id).GetStatus() != pb.OrderStatus_ORDER_STATUS_PENDING
	}
}

func TestKafkaPendingBecomesPlaced(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	topic, group := newTopic(t, brokers), uniq("g")
	id := uniq("ord")
	createPending(t, svc, id)

	rc := startConsumer(t, brokers, topic, group, svc)
	time.Sleep(2 * time.Second)
	if getOrder(t, svc, id).Status != pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatal("order changed without an event")
	}
	produceEvent(t, brokers, topic, id, "TRK-1", "TXN-1")

	until(t, "order PLACED", isPlaced(t, svc, id))
	o := getOrder(t, svc, id)
	if o.ShippingTrackingId != "TRK-1" || o.PaymentTransactionId != "TXN-1" || o.PlacedAt == nil {
		t.Errorf("order after event: %v", o)
	}
	rc.stop(t)
}

// The consumer starts while its topic does not exist (nobody has published yet): it creates the topic itself
// (CODE-09) and then handles the first event. The topic name carries KAFKA_TOPIC_PREFIX (FUT-03). The broker
// decides partitions and replication (the test broker: KAFKA_NUM_PARTITIONS=3).
func TestKafkaConsumerCreatesAMissingTopicWithPrefix(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	prefix := uniq("it") + "."
	cfg := events.ConfigFromEnv(func(k string) string {
		return map[string]string{"KAFKA_BROKERS": brokers, "KAFKA_TOPIC_PREFIX": prefix}[k]
	})
	if cfg.Topic != prefix+"orders.placed" {
		t.Fatalf("topic = %q", cfg.Topic)
	}
	id := uniq("ord")
	createPending(t, svc, id)

	rc := startConsumer(t, brokers, cfg.Topic, uniq("g"), svc) // the topic is NOT created by the test
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	until(t, "the consumer to create the topic", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return topicHasLeaders(ctx, cl, cfg.Topic, 3) == nil
	})
	if getOrder(t, svc, id).Status != pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatal("order changed without an event")
	}
	produceEvent(t, brokers, cfg.Topic, id, "TRK-1", "TXN-1")
	until(t, "order PLACED from the topic the consumer created", isPlaced(t, svc, id))
	rc.stop(t)
}

// A second start finds the topic and leaves it alone (TOPIC_ALREADY_EXISTS is success).
func TestKafkaConsumerLeavesAnExistingTopicAlone(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	topic := newTopic(t, brokers) // 3 partitions, created by the test
	id := uniq("ord")
	createPending(t, svc, id)
	rc := startConsumer(t, brokers, topic, uniq("g"), svc)
	produceEvent(t, brokers, topic, id, "TRK-1", "TXN-1")
	until(t, "order PLACED", isPlaced(t, svc, id))
	rc.stop(t)
}

func TestKafkaDuplicatesAndSkippedRecords(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	topic, group := newTopic(t, brokers), uniq("g")
	a, b, failed, other := uniq("a"), uniq("b"), uniq("f"), uniq("other")
	createPending(t, svc, a)
	createPending(t, svc, b)
	createPending(t, svc, failed)
	if _, err := svc.FailOrder(context.Background(), &pb.FailOrderRequest{OrderId: failed, Reason: "card declined"}); err != nil {
		t.Fatal(err)
	}
	// all records share one key, so they sit in one partition in this order
	k := []byte("k")
	produce(t, brokers, topic,
		[2][]byte{k, []byte("this is not json")},
		[2][]byte{k, []byte(`{"schema_version":2,"order_id":"x"}`)},
		[2][]byte{k, eventValue(a, "TRK-A", "TXN-A")},
		[2][]byte{k, eventValue(a, "TRK-A", "TXN-A")},      // duplicate: no-op
		[2][]byte{k, eventValue(a, "TRK-OTHER", "TXN-A")},  // same order, other tracking id: logged, not applied
		[2][]byte{k, eventValue(failed, "TRK-F", "TXN-F")}, // FAILED order: skipped
		[2][]byte{k, eventValue(other, "TRK-O", "TXN-O")},  // unknown order: skipped
		[2][]byte{k, eventValue(b, "TRK-B", "TXN-B")},
	)
	rc := startConsumer(t, brokers, topic, group, svc)
	until(t, "last event handled", isPlaced(t, svc, b))

	oa := getOrder(t, svc, a)
	if oa.ShippingTrackingId != "TRK-A" || oa.Status == pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Errorf("order a: %v", oa)
	}
	if of := getOrder(t, svc, failed); of.Status != pb.OrderStatus_ORDER_STATUS_FAILED || of.ShippingTrackingId != "" {
		t.Errorf("failed order was touched: %v", of)
	}
	if got := strings.Join(rc.h.calls(), ","); got != strings.Join([]string{a, a, a, failed, other, b}, ",") {
		t.Errorf("handler calls: %s", got)
	}
	rc.stop(t)
}

func TestKafkaRestartResumesAfterCommittedOffset(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	topic, group := newTopic(t, brokers), uniq("g")
	first, second := uniq("first"), uniq("second")
	createPending(t, svc, first)
	createPending(t, svc, second)

	produceEvent(t, brokers, topic, first, "TRK-1", "TXN-1")
	c1 := startConsumer(t, brokers, topic, group, svc)
	until(t, "first order PLACED", isPlaced(t, svc, first))
	c1.stop(t)

	produceEvent(t, brokers, topic, second, "TRK-2", "TXN-2")
	c2 := startConsumer(t, brokers, topic, group, svc)
	until(t, "second order PLACED", isPlaced(t, svc, second))
	c2.stop(t)

	if got := c1.h.calls(); len(got) != 1 || got[0] != first {
		t.Errorf("first consumer handled %v", got)
	}
	if got := c2.h.calls(); len(got) != 1 || got[0] != second {
		t.Errorf("restarted consumer must not see the committed record again, it handled %v", got)
	}
}

func TestKafkaTransientFailureIsRetriedNotSkipped(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	topic, group := newTopic(t, brokers), uniq("g")
	first, second := uniq("first"), uniq("second")
	createPending(t, svc, first)
	createPending(t, svc, second)
	svc.SetReady(false) // the service layer answers UNAVAILABLE, like a database that is down

	k := []byte("k")
	produce(t, brokers, topic, [2][]byte{k, eventValue(first, "TRK-1", "TXN-1")}, [2][]byte{k, eventValue(second, "TRK-2", "TXN-2")})
	rc := startConsumer(t, brokers, topic, group, svc)
	until(t, "the first record to be retried at least twice", func() bool { return len(rc.h.calls()) >= 2 })
	svc.SetReady(true)

	until(t, "both orders PLACED", func() bool { return isPlaced(t, svc, first)() && isPlaced(t, svc, second)() })
	for _, id := range rc.h.calls() { // nothing but the first record was tried while it was failing
		if id != first && id != second {
			t.Errorf("unexpected call %s", id)
		}
	}
	if rc.h.calls()[0] != first {
		t.Errorf("calls %v", rc.h.calls())
	}
	rc.stop(t)
}

// forwarder is a TCP proxy that can be started late, to stand in for a broker that is not reachable yet.
type forwarder struct {
	ln net.Listener
}

func startForwarder(t *testing.T, listen, target string) *forwarder {
	t.Helper()
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	f := &forwarder{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				u, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer func() { _ = u.Close() }()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(u, c); done <- struct{}{} }()
				go func() { _, _ = io.Copy(c, u); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return f
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func TestKafkaBrokerUnavailableAtStartThenAvailable(t *testing.T) {
	brokers := kafkaBrokers(t)
	svc, _, _ := setup(t)
	topic, group := newTopic(t, brokers), uniq("g")
	id := uniq("ord")
	createPending(t, svc, id)
	produceEvent(t, brokers, topic, id, "TRK-1", "TXN-1")

	// the consumer's only bootstrap address has nothing behind it yet
	addr := freeAddr(t)
	rc := startConsumer(t, addr, topic, group, svc)
	time.Sleep(3 * time.Second)
	if getOrder(t, svc, id).Status != pb.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatal("order changed while no broker was reachable")
	}
	startForwarder(t, addr, strings.Split(brokers, ",")[0]) // the broker "comes up"

	until(t, "consumer to recover and place the order", isPlaced(t, svc, id))
	rc.stop(t)
}

func TestKafkaConsumerRecoversFromBrokerRestart(t *testing.T) {
	brokers := kafkaBrokers(t)
	container := os.Getenv("TEST_KAFKA_CONTAINER")
	if container == "" {
		t.Skip("TEST_KAFKA_CONTAINER not set")
	}
	svc, _, _ := setup(t)
	topic, group := newTopic(t, brokers), uniq("g")
	before, after := uniq("before"), uniq("after")
	createPending(t, svc, before)
	createPending(t, svc, after)
	produceEvent(t, brokers, topic, before, "TRK-1", "TXN-1")
	rc := startConsumer(t, brokers, topic, group, svc)
	until(t, "first order PLACED", isPlaced(t, svc, before))

	if out, err := exec.Command("docker", "restart", "-t", "5", container).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v %s", err, out)
	}
	// the broker needs a while to come back; produce retries until it does
	var perr error
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		if perr = tryProduce(brokers, topic, after); perr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if perr != nil {
		t.Fatalf("broker did not come back: %v", perr)
	}
	// after the restart the consumer has to rejoin its group, which can take up to the group's rebalance timeout
	untilWithin(t, 180*time.Second, "order placed after the broker restart", isPlaced(t, svc, after))
	rc.stop(t)
	if got := rc.h.calls(); len(got) != 2 || got[0] != before || got[1] != after {
		t.Errorf("handled %v, want [before after] (no replay of the committed record)", got)
	}
}

func tryProduce(brokers, topic, orderID string) error {
	cl, err := tolerantProducer(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(orderID), Value: eventValue(orderID, "TRK-2", "TXN-2")}).FirstErr(); err != nil {
		return errors.Join(err)
	}
	return nil
}
