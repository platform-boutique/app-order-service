package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"order-service/internal/obs"
	"order-service/internal/orders"
	pb "order-service/proto"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

const serviceName = "order-service"

// log is the process logger; obs.Start gives it the contract's JSON shape, the level from LOG_LEVEL and
// (with an OTLP endpoint) the OTLP hook. The same logger goes to the orders, events and Kafka code.
var log = logrus.New()

func main() {
	port := "8085"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	// `order-service healthcheck` is the container health probe: exit 0 only while the server reports SERVING.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(port))
	}

	// Env contract (OTEL_*, PYROSCOPE_ADDR), JSON logs, OTLP pipelines, Pyroscope, build_info, /metrics on
	// :9090 (METRICS_PORT) and pprof only with PPROF_ADDR all live in internal/obs.
	o := obs.Start(context.Background(), obs.Options{Service: serviceName, Loggers: []*logrus.Logger{log}, ShutdownTimeout: obsFlushBudget})
	defer o.Recover()
	defer o.Shutdown()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		o.Fatal("DATABASE_URL is not set", errors.New("DATABASE_URL is required"))
	}

	if err := run(port, dsn); err != nil {
		o.Fatal("server error", err)
	}
	log.Info("stopped")
}

func run(port, dsn string) error {
	stages, err := stagesFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	log.Infof("status stages after confirmation: PROCESSING %v, SHIPPED %v, DELIVERED %v", stages.Processing, stages.Shipped, stages.Delivered)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err) // the error never contains the password
	}
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	}
	obs.InstrumentPool(cfg, nil)                                  // database client spans (children of the request or consumer span)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg) // lazy: does not connect yet
	if err != nil {
		return fmt.Errorf("database pool: %w", err)
	}
	// closed by the shutdown steps, after the consumer and the gRPC calls are done; this defer covers early returns
	defer pool.Close()
	if err := obs.RegisterPoolMetrics(prometheus.DefaultRegisterer, pool); err != nil {
		log.WithError(err).Warn("database pool metrics not registered")
	}
	requestTimeout, badTimeout := obs.RequestTimeoutFromEnv(os.Getenv)
	if badTimeout != "" {
		log.Warnf("REQUEST_TIMEOUT=%q is not a duration of at least 100ms (or 0), using %s", badTimeout, requestTimeout)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	// gRPC server with OTel tracing + Prometheus metrics interceptors, plus a panic guard
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(obs.UnaryServerInterceptor(), obs.DeadlineUnaryInterceptor(requestTimeout), grpcprom.UnaryServerInterceptor, recoverUnary),
		grpc.ChainStreamInterceptor(obs.StreamServerInterceptor(), grpcprom.StreamServerInterceptor),
	)

	store := orders.NewPGStore(pool)
	svc := orders.NewService(store, log, stages, time.Now)
	pb.RegisterOrderServiceServer(srv, svc)

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	setHealth(healthSrv, healthpb.HealthCheckResponse_NOT_SERVING)
	reflection.Register(srv)

	grpcprom.Register(srv)
	grpcprom.EnableHandlingTimeHistogram()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// --- Database: migrate (retrying until the DB is reachable), then watch it ---
	go watchDatabase(ctx, pool, store, svc, healthSrv)

	// --- Kafka: orders.placed consumer (self-healing confirm), only with KAFKA_BROKERS set; stopped before the pool closes ---
	stopKafka, err := startKafka(ctx, svc)
	if err != nil {
		return err
	}
	defer stopKafka(drainBudget)

	serveErr := make(chan error, 1)
	go func() {
		log.Infof("starting grpc server on %s", listener.Addr().String())
		serveErr <- srv.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	log.Info("shutdown signal received, draining")
	watchdog(time.After, os.Exit, log)
	healthSrv.Shutdown() // NOT_SERVING for every service, so probes fail first
	(&shutdownSteps{
		GracefulStop: srv.GracefulStop,
		ForceStop:    srv.Stop,
		StopConsumer: stopKafka,
		ClosePool:    pool.Close,
		After:        time.After,
		Log:          log,
	}).run()
	return nil
}

func setHealth(h *health.Server, st healthpb.HealthCheckResponse_ServingStatus) {
	h.SetServingStatus("", st)
	h.SetServingStatus(pb.OrderService_ServiceDesc.ServiceName, st)
}

// watchDatabase runs the migrations until they succeed, then pings the database
// every few seconds and keeps the health status (and the handlers) in line with it.
func watchDatabase(ctx context.Context, pool *pgxpool.Pool, store orders.Store, svc *orders.Service, h *health.Server) {
	for attempt := 1; ; attempt++ {
		applied, err := orders.Setup(ctx, pool)
		if err == nil {
			log.Infof("database ready: %d migration(s) applied", applied)
			break
		}
		if ctx.Err() != nil {
			return
		}
		log.Warnf("database not ready (attempt %d), retrying in 2s: %v", attempt, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	serving := false
	check := func() {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := store.Ping(pctx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		switch {
		case err == nil && !serving:
			serving = true
			svc.SetReady(true)
			setHealth(h, healthpb.HealthCheckResponse_SERVING)
			log.Info("health: SERVING")
		case err != nil && serving:
			serving = false
			svc.SetReady(false)
			setHealth(h, healthpb.HealthCheckResponse_NOT_SERVING)
			log.Warnf("health: NOT_SERVING, database unreachable: %v", err)
		}
	}
	check()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			check()
		}
	}
}

// recoverUnary turns a handler panic into INTERNAL instead of crashing the process.
func recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.WithContext(ctx).WithFields(logrus.Fields{"method": info.FullMethod, "stack": string(debug.Stack())}).WithError(fmt.Errorf("%v", r)).Error("panic in handler")
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}
