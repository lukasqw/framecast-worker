package observability

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

func NewLogger() *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.MessageKey:
				a.Key = "message"
			case slog.LevelKey:
				a.Key = "status"
				if level, ok := a.Value.Any().(slog.Level); ok {
					a.Value = slog.StringValue(strings.ToLower(level.String()))
				}
			case slog.TimeKey:
				a.Key = "timestamp"
			}
			return a
		},
	})).With(
		slog.String("service", envOrDefault("DD_SERVICE", "framecast-worker")),
		slog.String("env", os.Getenv("APP_ENV")),
		slog.String("version", os.Getenv("APP_VERSION")),
	)
	slog.SetDefault(logger)
	return logger
}

func LoggerFromContext(ctx context.Context) *slog.Logger {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return slog.Default()
	}

	sc := span.SpanContext()

	traceIDBytes := sc.TraceID()
	ddTraceID := binary.BigEndian.Uint64(traceIDBytes[8:])

	spanIDBytes := sc.SpanID()
	ddSpanID := binary.BigEndian.Uint64(spanIDBytes[:])

	return slog.Default().With(
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
		slog.String("dd.trace_id", fmt.Sprintf("%d", ddTraceID)),
		slog.String("dd.span_id", fmt.Sprintf("%d", ddSpanID)),
	)
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
