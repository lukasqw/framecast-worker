package observability

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestNewLogger_RetornaLoggerEDefinePadrao(t *testing.T) {
	logger := NewLogger()
	require.NotNil(t, logger)
	assert.Same(t, logger, slog.Default())
}

func TestLoggerFromContext_SemSpan_RetornaDefault(t *testing.T) {
	logger := LoggerFromContext(context.Background())
	require.NotNil(t, logger)
}

func TestLoggerFromContext_ComSpanValido_AdicionaTraceID(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger := LoggerFromContext(ctx)
	require.NotNil(t, logger)
}

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("FRAMECAST_TEST_VAR", "valor")
	assert.Equal(t, "valor", envOrDefault("FRAMECAST_TEST_VAR", "padrao"))

	t.Setenv("FRAMECAST_TEST_VAR_VAZIA", "")
	assert.Equal(t, "padrao", envOrDefault("FRAMECAST_TEST_VAR_VAZIA", "padrao"))
}
