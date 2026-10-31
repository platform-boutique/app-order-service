package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoSource is the franz-go implementation of Source: one group member, offsets committed by hand.
type kgoSource struct {
	cl  *kgo.Client
	log *logrus.Logger
}

// NewKafkaSource connects (lazily: an unreachable broker is not an error here, Poll reports it and the consumer backs
// off) as a member of cfg.Group on Topic. A new group starts at the earliest offset. Auto commit is off and
// rebalances are blocked while a polled batch is being handled, so a partition is never taken away mid-batch.
func NewKafkaSource(cfg Config, log *logrus.Logger) (Source, error) {
	if cfg.Topic == "" {
		cfg.Topic = Topic
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
	return &kgoSource{cl: cl, log: log}, nil
}

func (s *kgoSource) Poll(ctx context.Context) ([]Record, error) {
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
			out = append(out, Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
				HighWatermark: p.HighWatermark, raw: r})
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
