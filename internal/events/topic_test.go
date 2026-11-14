package events

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fakeBroker answers CreateTopics with a fixed error code (or fails the request) and remembers what was asked.
type fakeBroker struct {
	code  int16
	err   error
	calls int
	got   *kmsg.CreateTopicsRequest
}

func (f *fakeBroker) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	f.calls++
	f.got = req.(*kmsg.CreateTopicsRequest)
	if f.err != nil {
		return nil, f.err
	}
	resp := kmsg.NewCreateTopicsResponse()
	rt := kmsg.NewCreateTopicsResponseTopic()
	rt.Topic = f.got.Topics[0].Topic
	rt.ErrorCode = f.code
	resp.Topics = append(resp.Topics, rt)
	return &resp, nil
}

func TestEnsureTopicAsksForTheBrokerDefaults(t *testing.T) {
	b := &fakeBroker{}
	if err := ensureTopic(context.Background(), b, "dev.orders.placed"); err != nil {
		t.Fatal(err)
	}
	r := b.got.Topics[0]
	if r.Topic != "dev.orders.placed" || r.NumPartitions != -1 || r.ReplicationFactor != -1 {
		t.Fatalf("request: %+v", r)
	}
	if b.got.TimeoutMillis <= 0 {
		t.Errorf("no broker-side timeout: %d", b.got.TimeoutMillis)
	}
}

func TestEnsureTopicExistingTopicIsSuccess(t *testing.T) {
	if err := ensureTopic(context.Background(), &fakeBroker{code: kerr.TopicAlreadyExists.Code}, "orders.placed"); err != nil {
		t.Fatalf("existing topic: %v", err)
	}
}

func TestEnsureTopicErrors(t *testing.T) {
	err := ensureTopic(context.Background(), &fakeBroker{code: kerr.TopicAuthorizationFailed.Code}, "orders.placed")
	if !errors.Is(err, errTopicNotAllowed) {
		t.Errorf("authorization failure = %v, want errTopicNotAllowed", err)
	}
	err = ensureTopic(context.Background(), &fakeBroker{code: kerr.InvalidReplicationFactor.Code}, "orders.placed")
	if err == nil || errors.Is(err, errTopicNotAllowed) {
		t.Errorf("replication error = %v, want a plain error", err)
	}
	boom := errors.New("broker unreachable")
	if err := ensureTopic(context.Background(), &fakeBroker{err: boom}, "orders.placed"); !errors.Is(err, boom) {
		t.Errorf("request error = %v, want the broker error wrapped", err)
	}
}

func newSource(b requester) *kgoSource {
	l := logrus.New()
	l.SetOutput(discard{})
	return &kgoSource{creator: b, log: l, topic: "orders.placed"}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// While the broker cannot be asked, every Poll tries again; after a success it is not asked any more.
func TestSourceCreatesTheTopicOnceBrokerAnswers(t *testing.T) {
	b := &fakeBroker{err: errors.New("down")}
	s := newSource(b)
	if err := s.ensure(context.Background()); err == nil || s.topicReady {
		t.Fatalf("down broker: err=%v ready=%v", err, s.topicReady)
	}
	b.err = nil
	if err := s.ensure(context.Background()); err != nil || !s.topicReady {
		t.Fatalf("up broker: err=%v ready=%v", err, s.topicReady)
	}
	calls := b.calls
	if err := s.ensure(context.Background()); err != nil || b.calls != calls {
		t.Fatalf("asked again after success: calls %d -> %d (err %v)", calls, b.calls, err)
	}
}

func TestSourceGoesOnWhenCreatingIsNotAllowed(t *testing.T) {
	s := newSource(&fakeBroker{code: kerr.TopicAuthorizationFailed.Code})
	if err := s.ensure(context.Background()); err != nil || !s.topicReady {
		t.Fatalf("err=%v ready=%v: an ACL that forbids creating must not stop the consumer", err, s.topicReady)
	}
}
