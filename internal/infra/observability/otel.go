package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var otelInitialized bool

// OTelInitialized indica se o provider foi inicializado com exporter real.
func OTelInitialized() bool { return otelInitialized }

// InitOTel configura TracerProvider + MeterProvider com exportadores OTLP gRPC.
// Endpoint vazio → modo no-op (dev sem Datadog).
func InitOTel(ctx context.Context) (shutdown func(context.Context) error, err error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		slog.Info("OTel desabilitado: OTEL_EXPORTER_OTLP_ENDPOINT não configurado")
		return func(context.Context) error { return nil }, nil
	}

	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")

	service := os.Getenv("DD_SERVICE")
	if service == "" {
		service = "framecast-worker"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(service),
			semconv.ServiceVersion(os.Getenv("APP_VERSION")),
			attribute.String("deployment.environment", os.Getenv("APP_ENV")),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar OTel resource: %w", err)
	}

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("falha ao conectar ao OTel endpoint %q: %w", endpoint, err)
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("falha ao criar trace exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tracerProvider)

	// Delta temporality — requisito do Datadog Agent
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithGRPCConn(conn),
		otlpmetricgrpc.WithTemporalitySelector(func(_ sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar metric exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(30*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if err := initMetrics(Meter()); err != nil {
		return nil, fmt.Errorf("falha ao inicializar métricas: %w", err)
	}

	otelInitialized = true

	return func(ctx context.Context) error {
		var errs []error
		if err := tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		if err := meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}, nil
}

func Tracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer("framecast-worker")
}

func Meter() metric.Meter {
	return otel.GetMeterProvider().Meter("framecast-worker")
}

func SpanWorker(ctx context.Context, operation string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, operation)
}

// SpanConsumer cria um span com SpanKindConsumer para recebimento de mensagens SQS.
func SpanConsumer(ctx context.Context, operation string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, operation, trace.WithSpanKind(trace.SpanKindConsumer))
}

// InitNoop inicializa OTel com providers no-op — para testes que precisam de spans
// ativos sem exporter real. Retorna cleanup que reverte otelInitialized para false.
func InitNoop() func() {
	otel.SetTracerProvider(tracenoop.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	otelInitialized = true
	return func() { otelInitialized = false }
}

// Métricas de negócio do worker
var (
	videoProcessedCounter  metric.Int64Counter
	videoProcessingHisto   metric.Float64Histogram
	videoFrameCountHisto   metric.Int64Histogram
	ffmpegDurationHisto    metric.Float64Histogram
	sqsMessagesCounter     metric.Int64Counter
)

func initMetrics(m metric.Meter) error {
	var err error

	if videoProcessedCounter, err = m.Int64Counter(
		"framecast.video.processed.total",
		metric.WithDescription("Total de vídeos processados"),
	); err != nil {
		return err
	}

	if videoProcessingHisto, err = m.Float64Histogram(
		"framecast.video.processing.duration",
		metric.WithDescription("Duração total de processamento de vídeo em segundos"),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}

	if videoFrameCountHisto, err = m.Int64Histogram(
		"framecast.video.frame_count",
		metric.WithDescription("Número de frames extraídos por vídeo"),
	); err != nil {
		return err
	}

	if ffmpegDurationHisto, err = m.Float64Histogram(
		"framecast.ffmpeg.duration",
		metric.WithDescription("Duração da execução do FFmpeg em segundos"),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}

	if sqsMessagesCounter, err = m.Int64Counter(
		"framecast.worker.sqs.messages.received",
		metric.WithDescription("Total de mensagens SQS recebidas pelo worker"),
	); err != nil {
		return err
	}

	return nil
}

func RecordVideoProcessed(ctx context.Context, status string) {
	if videoProcessedCounter != nil {
		videoProcessedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
	}
}

func RecordVideoProcessingDuration(ctx context.Context, seconds float64, status string) {
	if videoProcessingHisto != nil {
		videoProcessingHisto.Record(ctx, seconds, metric.WithAttributes(attribute.String("status", status)))
	}
}

func RecordFrameCount(ctx context.Context, count int64) {
	if videoFrameCountHisto != nil {
		videoFrameCountHisto.Record(ctx, count)
	}
}

func RecordFFmpegDuration(ctx context.Context, seconds float64) {
	if ffmpegDurationHisto != nil {
		ffmpegDurationHisto.Record(ctx, seconds)
	}
}

func RecordSQSMessagesReceived(ctx context.Context, count int64) {
	if sqsMessagesCounter != nil {
		sqsMessagesCounter.Add(ctx, count)
	}
}
