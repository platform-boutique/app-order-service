package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"order-service/internal/obs"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// kgoSource is the franz-go implementation of Source: one group member, offsets committed by hand.
type kgoSource struct {
	cl      *kgo.Client
	creator requester // cl; tests inject a fake
	log     *logrus.Logger
	topic   string
	// topicReady is set once the topic exists (or may not be created by this service); until then every Poll
	// tries to create it. Poll is called by one goroutine, so no lock.
	topicReady bool
}

// createTopicTimeout bounds one CreateTopics request.
const createTopicTimeout = 10 * time.Second

// NewKafkaSource connects (lazily: an unreachable broker is not an error here, Poll reports it and the consumer backs
// off) as a member of cfg.Group on Topic. A new group starts at the earliest offset. Auto commit is off and
// rebalances are blocked while a polled batch is being handled, so a partition is never taken away mid-batch.
func NewKafkaSource(cfg Config, log *logrus.Logger) (Source, error) {
	if cfg.Topic == "" {
		cfg.Topic = cfg.TopicPrefix + Topic
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// a topic that does not exist yet (nobody has published) is picked up within this time once it appears
		kgo.MetadataMaxAge(15*time.Second),
		kgo.WithLogger(kgoLogger{log}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &kgoSource{cl: cl, creator: cl, log: log, topic: cfg.Topic}, nil
}

// requester is what ensureTopic needs from the client (*kgo.Client); tests inject a fake.
type requester interface {
	Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error)
}

// errTopicNotAllowed means the broker refused to create the topic for this client (ACL). The topic may exist anyway.
var errTopicNotAllowed = errors.New("not allowed to create the topic")

// ensureTopic creates the topic when it is missing (CODE-09). Partition count and replication factor are the
// broker's defaults (-1); an existing topic is left as it is. Concurrent starters are fine: TOPIC_ALREADY_EXISTS is
// success.
func ensureTopic(ctx context.Context, cl requester, topic string) error {
	req := kmsg.NewCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = int32(createTopicTimeout / time.Millisecond)
	resp, err := cl.Request(ctx, &req)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	r, ok := resp.(*kmsg.CreateTopicsResponse)
	if !ok {
		return fmt.Errorf("create topic %s: unexpected response %T", topic, resp)
	}
	for _, t := range r.Topics {
		switch ke := kerr.ErrorForCode(t.ErrorCode); {
		case ke == nil, errors.Is(ke, kerr.TopicAlreadyExists):
		case errors.Is(ke, kerr.TopicAuthorizationFailed):
			return fmt.Errorf("%w %s: %v", errTopicNotAllowed, topic, ke)
		default:
			return fmt.Errorf("create topic %s: %w", topic, ke)
		}
	}
	return nil
}

func (s *kgoSource) ensure(ctx context.Context) error {
	if s.topicReady {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, createTopicTimeout+2*time.Second)
	defer cancel()
	switch err := ensureTopic(cctx, s.creator, s.topic); {
	case err == nil:
		s.log.Infof("kafka topic %s exists", s.topic)
	case errors.Is(err, errTopicNotAllowed):
		// the topic may have been created by someone else: keep going, the fetch tells
		s.log.Warnf("%v; assuming it exists", err)
	default:
		return err
	}
	s.topicReady = true
	return nil
}

func (s *kgoSource) Poll(ctx context.Context) ([]Record, error) {
	if err := s.ensure(ctx); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	fetches := s.cl.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, ErrSourceClosed
	}
	if ctx.Err() != nil {
		s.cl.AllowRebalance()
		return nil, ctx.Err()
	}
	var errs []error
	topicMissing := false
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID) {
			topicMissing = true
			return
		}
		errs = append(errs, fmt.Errorf("%s/%d: %w", topic, partition, err))
	})
	var out []Record
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, r := range p.Records {
			hs := make([]obs.KafkaHeader, len(r.Headers))
			for i, h := range r.Headers {
				hs[i] = obs.KafkaHeader{Key: h.Key, Value: h.Value}
			}
			out = append(out, Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
				HighWatermark: p.HighWatermark, Headers: hs, raw: r})
		}
	})
	if len(out) == 0 {
		s.cl.AllowRebalance()
		if len(errs) == 0 && topicMissing {
			return nil, ErrTopicNotAvailable
		}
		return nil, errors.Join(errs...)
	}
	if len(errs) > 0 {
		s.log.Warnf("kafka fetch errors alongside records: %v", errors.Join(errs...))
	}
	return out, nil
}

func (s *kgoSource) Finish(ctx context.Context, handled []Record) error {
	defer s.cl.AllowRebalance()
	if len(handled) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(handled))
	for i, h := range handled {
		recs[i] = h.raw.(*kgo.Record)
	}
	return s.cl.CommitRecords(ctx, recs...)
}

func (s *kgoSource) Close() { s.cl.CloseAllowingRebalance() }

// kgoLogger passes franz-go's warnings and errors to logrus (its info/debug chatter stays off).
type kgoLogger struct{ l *logrus.Logger }

// Level follows the logrus level, but never goes below warn: info and debug chatter only with a debug/trace logger.
func (k kgoLogger) Level() kgo.LogLevel {
	switch {
	case k.l.IsLevelEnabled(logrus.DebugLevel):
		return kgo.LogLevelDebug
	default:
		return kgo.LogLevelWarn
	}
}

func (k kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	f := logrus.Fields{}
	for i := 0; i+1 < len(keyvals); i += 2 {
		f[fmt.Sprint(keyvals[i])] = keyvals[i+1]
	}
	e := k.l.WithFields(f)
	switch level {
	case kgo.LogLevelError:
		e.Error("kafka client: " + msg)
	case kgo.LogLevelWarn:
		e.Warn("kafka client: " + msg)
	default:
		e.Debug("kafka client: " + msg)
	}
}
