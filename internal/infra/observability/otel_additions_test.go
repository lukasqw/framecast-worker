package observability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestOTelInitialized_RetornaFalseSemInit(t *testing.T) {
	orig := otelInitialized
	otelInitialized = false
	defer func() { otelInitialized = orig }()

	assert.False(t, OTelInitialized())
}

func TestSpanConsumer_FuncionaSemInit(t *testing.T) {
	ctx, span := SpanConsumer(context.Background(), "sqs.receive-teste")
	require.NotNil(t, ctx)
	require.NotNil(t, span)
	assert.NotPanics(t, func() { span.End() })
}

func TestInitMetrics_RegistraSQSMessagesCounter(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	err := initMetrics(meter)
	require.NoError(t, err)

	assert.NotNil(t, sqsMessagesCounter)
}

func TestRecordSQSMessagesReceived_SemInstrumento_NaoPanica(t *testing.T) {
	orig := sqsMessagesCounter
	sqsMessagesCounter = nil
	defer func() { sqsMessagesCounter = orig }()

	assert.NotPanics(t, func() {
		RecordSQSMessagesReceived(context.Background(), 5)
	})
}

func TestRecordSQSMessagesReceived_ComInstrumento_NaoPanica(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	require.NoError(t, initMetrics(meter))

	assert.NotPanics(t, func() {
		RecordSQSMessagesReceived(context.Background(), 3)
	})
}

func TestRecordHeartbeatFuncs_SemInstrumento_NaoPanica(t *testing.T) {
	origSQS := heartbeatSQSErrCounter
	origDB := heartbeatDBErrCounter
	origPanic := heartbeatPanicCounter
	origAbandoned := processingAbandonedCounter
	origConsumerPanic := consumerPanicCounter
	heartbeatSQSErrCounter = nil
	heartbeatDBErrCounter = nil
	heartbeatPanicCounter = nil
	processingAbandonedCounter = nil
	consumerPanicCounter = nil
	defer func() {
		heartbeatSQSErrCounter = origSQS
		heartbeatDBErrCounter = origDB
		heartbeatPanicCounter = origPanic
		processingAbandonedCounter = origAbandoned
		consumerPanicCounter = origConsumerPanic
	}()

	ctx := context.Background()
	assert.NotPanics(t, func() { RecordHeartbeatSQSError(ctx) })
	assert.NotPanics(t, func() { RecordHeartbeatDBError(ctx) })
	assert.NotPanics(t, func() { RecordHeartbeatPanic(ctx) })
	assert.NotPanics(t, func() { RecordProcessingAbandoned(ctx) })
	assert.NotPanics(t, func() { RecordConsumerPanic(ctx) })
}

func TestRecordHeartbeatFuncs_ComInstrumento_NaoPanica(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	require.NoError(t, initMetrics(meter))

	ctx := context.Background()
	assert.NotPanics(t, func() { RecordHeartbeatSQSError(ctx) })
	assert.NotPanics(t, func() { RecordHeartbeatDBError(ctx) })
	assert.NotPanics(t, func() { RecordHeartbeatPanic(ctx) })
	assert.NotPanics(t, func() { RecordProcessingAbandoned(ctx) })
	assert.NotPanics(t, func() { RecordConsumerPanic(ctx) })
}

func TestInitOTel_EndpointFake_CobreCorpoDaFuncao(t *testing.T) {
	// gRPC com NewClient é lazy: a conexão não é estabelecida durante InitOTel,
	// então a função completa sem bloquear mesmo com porta fechada.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:19999")
	t.Setenv("DD_SERVICE", "test-worker")
	t.Setenv("APP_VERSION", "0.0.1")
	t.Setenv("APP_ENV", "test")

	orig := otelInitialized
	defer func() { otelInitialized = orig }()

	shutdown, err := InitOTel(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.True(t, otelInitialized)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = shutdown(ctx)
}

func TestInitNoop_HabilitaOTelERetornaCleanup(t *testing.T) {
	orig := otelInitialized
	defer func() { otelInitialized = orig }()

	shutdown := InitNoop()
	assert.True(t, otelInitialized)
	shutdown()
	assert.False(t, otelInitialized)
}
