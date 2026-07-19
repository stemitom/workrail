package observability

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workrail_jobs_enqueued_total",
		Help: "Total jobs enqueued.",
	}, []string{"queue", "workflow_type"})

	JobsClaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workrail_jobs_claimed_total",
		Help: "Total jobs claimed by workers.",
	}, []string{"queue", "workflow_type"})

	JobsSucceeded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workrail_jobs_succeeded_total",
		Help: "Total jobs completed successfully by workers.",
	}, []string{"queue", "workflow_type"})

	JobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workrail_jobs_failed_total",
		Help: "Total jobs failed by workers. Failures may retry or dead-letter depending on attempts.",
	}, []string{"queue", "workflow_type"})

	JobHeartbeats = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workrail_job_heartbeats_total",
		Help: "Total worker heartbeats recorded for running jobs.",
	}, []string{"queue"})

	WorkerInFlightJobs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "workrail_worker_inflight_jobs",
		Help: "Current in-flight jobs by worker queue and workflow type.",
	}, []string{"queue", "workflow_type"})

	WorkerConfiguredConcurrency = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "workrail_worker_configured_concurrency",
		Help: "Configured worker concurrency by worker id and queue.",
	}, []string{"worker_id", "queue"})

	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "workrail_http_requests_total",
		Help: "Total HTTP requests.",
	}, []string{"method", "path", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "workrail_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})
)

type QueueDepthReader interface {
	QueueDepth(context.Context) ([]QueueDepth, error)
}

type QueueDepth struct {
	Queue  string `json:"queue"`
	Status string `json:"status"`
	Count  int64  `json:"count"`
}

type TracingConfig struct {
	ServiceName string
	Enabled     bool
	Endpoint    string
	Insecure    bool
}

var queueDepthCollectors sync.Map

func RegisterQueueDepthCollector(reader QueueDepthReader) {
	if reader == nil {
		return
	}
	if _, loaded := queueDepthCollectors.LoadOrStore(reader, struct{}{}); loaded {
		return
	}
	collector := &queueDepthCollector{
		reader: reader,
		desc: prometheus.NewDesc(
			"workrail_queue_depth",
			"Current number of jobs by queue and status.",
			[]string{"queue", "status"},
			nil,
		),
	}
	if err := prometheus.Register(collector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			otel.Handle(err)
		}
	}
}

func Init(ctx context.Context, cfg TracingConfig) (func(context.Context) error, error) {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {}))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	if !cfg.Enabled || cfg.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	endpoint, insecureTransport := normalizeOTLPEndpoint(cfg.Endpoint, cfg.Insecure)

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
	}
	if insecureTransport {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func normalizeOTLPEndpoint(endpoint string, insecureTransport bool) (string, bool) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return endpoint, insecureTransport
	}
	switch parsed.Scheme {
	case "http":
		return parsed.Host, true
	case "https":
		return parsed.Host, false
	default:
		return endpoint, insecureTransport
	}
}

func HTTPMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		// The matched mux pattern keeps label cardinality bounded; the raw
		// path would mint one series per job UUID.
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type queueDepthCollector struct {
	reader QueueDepthReader
	desc   *prometheus.Desc
}

func (c *queueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *queueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	depths, err := c.reader.QueueDepth(ctx)
	if err != nil {
		otel.Handle(err)
		return
	}
	for _, depth := range depths {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(depth.Count),
			depth.Queue,
			depth.Status,
		)
	}
}
