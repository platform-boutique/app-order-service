package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"order-service/internal/obs"
	"order-service/internal/orders"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

func env(m map[string]string) obs.Getenv { return func(k string) string { return m[k] } }

// TestEnvContract is order-service's view of the telemetry contract (OBS-01, OBS-04). The full matrix is in
// internal/obs/env_test.go; this table pins what main.go relies on: the names read, and the legacy ones that are not.
func TestEnvContract(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		telemetry string // expected OTLP endpoint, "" = off
		profiling string // expected Pyroscope address, "" = off
	}{
		{"nothing set", nil, "", ""},
		{"endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317"}, "http://alloy:4317", ""},
		{"blank endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "  "}, "", ""},
		{"endpoint without scheme is off, not a crash", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "alloy:4317"}, "", ""},
		{"legacy COLLECTOR_SERVICE_ADDR is ignored", map[string]string{"COLLECTOR_SERVICE_ADDR": "alloy:4317"}, "", ""},
		{"legacy ENABLE_TRACING=0 is no longer read", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "ENABLE_TRACING": "0"}, "http://alloy:4317", ""},
		{"sdk disabled", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": "true", "PYROSCOPE_ADDR": "http://pyroscope:4040"}, "", ""},
		{"sdk disabled, upper case and padded", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": " TRUE "}, "", ""},
		{"sdk disabled=false", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": "false"}, "http://alloy:4317", ""},
		{"ENABLE_PROFILING=1 alone starts nothing", map[string]string{"ENABLE_PROFILING": "1"}, "", ""},
		{"pyroscope", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040"}, "", "http://pyroscope:4040"},
		{"pyroscope, ENABLE_PROFILING=1", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040", "ENABLE_PROFILING": "1"}, "", "http://pyroscope:4040"},
		{"pyroscope, ENABLE_PROFILING=0", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040", "ENABLE_PROFILING": "0"}, "", ""},
		{"pyroscope without scheme", map[string]string{"PYROSCOPE_ADDR": "pyroscope:4040"}, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := obs.LoadSettings(serviceName, env(c.env))
			if s.OTLPEndpoint != c.telemetry || s.TelemetryOn() != (c.telemetry != "") {
				t.Errorf("telemetry endpoint = %q (on=%v), want %q", s.OTLPEndpoint, s.TelemetryOn(), c.telemetry)
			}
			if s.PyroscopeAddr != c.profiling || s.ProfilingOn() != (c.profiling != "") {
				t.Errorf("profiling addr = %q (on=%v), want %q", s.PyroscopeAddr, s.ProfilingOn(), c.profiling)
			}
		})
	}
}

func TestEnvDefaultsAndResource(t *testing.T) {
	s := obs.LoadSettings(serviceName, env(map[string]string{"HOSTNAME": "order-0"}))
	if s.Service != "order-service" || s.Environment != "local" || s.Instance != "order-0" || s.MetricsAddr != ":9090" || s.PprofAddr != "" {
		t.Fatalf("defaults: %+v", s)
	}
	s = obs.LoadSettings(serviceName, env(map[string]string{"DEPLOYMENT_ENVIRONMENT": "dev", "METRICS_PORT": "9191", "PPROF_ADDR": "127.0.0.1:6060", "SERVICE_VERSION": "1.2.3"}))
	if s.Environment != "dev" || s.MetricsAddr != ":9191" || s.PprofAddr != "127.0.0.1:6060" || s.Version != "1.2.3" {
		t.Fatalf("overrides: %+v", s)
	}
}

// TestStartWiringLogsTheContractShape starts the kit the way main does (same options, no listener) and checks the
// first line is the JSON "starting" line with the service name, and that a handler-style call carries trace ids.
func TestStartWiringLogsTheContractShape(t *testing.T) {
	for _, k := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SDK_DISABLED", "PYROSCOPE_ADDR", "ENABLE_PROFILING", "LOG_LEVEL"} {
		t.Setenv(k, "")
	}
	var out bytes.Buffer
	l := logrus.New()
	x := obs.Start(context.Background(), obs.Options{
		Service: serviceName, Loggers: []*logrus.Logger{l}, Out: &out, NoMetricsServer: true,
		Registerer: prometheus.NewRegistry(), Gatherer: prometheus.NewRegistry(),
	})
	defer x.Shutdown()

	l.WithContext(obs.WithRequestID(context.Background(), "req-1")).Info("no span here")
	var first, second map[string]any
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("first line is not JSON: %v: %s", err, lines[0])
	}
	if first["message"] != "starting" || first["service"] != serviceName || first["severity"] != "info" || first["version"] == nil || first["commit"] == nil {
		t.Fatalf("starting line: %v", first)
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &second); err != nil {
		t.Fatalf("last line is not JSON: %v", err)
	}
	if second["request_id"] != "req-1" || second["trace_id"] != nil || second["span_id"] != nil {
		t.Fatalf("outside a span the ids must be absent: %v", second)
	}
}

func TestStagesFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	s, err := stagesFromEnv(env(nil))
	if err != nil || s != orders.DefaultStages {
		t.Fatalf("defaults: %v, %v", s, err)
	}
	s, err = stagesFromEnv(env(map[string]string{"ORDER_PROCESSING_AFTER": "10s", "ORDER_SHIPPED_AFTER": " 30s ", "ORDER_DELIVERED_AFTER": "2m"}))
	if err != nil || s.Processing != 10*time.Second || s.Shipped != 30*time.Second || s.Delivered != 2*time.Minute {
		t.Fatalf("custom: %v, %v", s, err)
	}
	bad := []map[string]string{
		{"ORDER_PROCESSING_AFTER": "soon"},
		{"ORDER_PROCESSING_AFTER": "0s"},
		{"ORDER_PROCESSING_AFTER": "-1m"},
		{"ORDER_SHIPPED_AFTER": "30s"},                               // not after the default processing offset of 1m
		{"ORDER_DELIVERED_AFTER": "3m"},                              // equal to shipped
		{"ORDER_PROCESSING_AFTER": "5m"},                             // beyond the default shipped
		{"ORDER_SHIPPED_AFTER": "5m", "ORDER_DELIVERED_AFTER": "4m"}, // delivered before shipped
	}
	for _, m := range bad {
		if _, err := stagesFromEnv(env(m)); err == nil {
			t.Errorf("%v accepted", m)
		}
	}
}
